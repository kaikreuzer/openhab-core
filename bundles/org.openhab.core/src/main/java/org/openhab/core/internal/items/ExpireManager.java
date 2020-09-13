/**
 * Copyright (c) 2010-2020 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.core.internal.items;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.core.common.ThreadPoolManager;
import org.openhab.core.events.Event;
import org.openhab.core.events.EventFilter;
import org.openhab.core.events.EventPublisher;
import org.openhab.core.events.EventSubscriber;
import org.openhab.core.items.Item;
import org.openhab.core.items.MetadataRegistry;
import org.openhab.core.items.events.ItemCommandEvent;
import org.openhab.core.items.events.ItemEventFactory;
import org.openhab.core.items.events.ItemStateEvent;
import org.openhab.core.types.Command;
import org.openhab.core.types.State;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component which takes care of sending item state expiry event.
 *
 * @author Kai Kreuzer - Initial contribution
 */
@NonNullByDefault
@Component(immediate = true, service = { ExpireManager.class,
        EventSubscriber.class }, configurationPid = "org.openhab.expire", configurationPolicy = ConfigurationPolicy.OPTIONAL)
public class ExpireManager implements EventSubscriber {

    protected static final String EVENT_SOURCE = "org.openhab.core.expire";

    protected static final String PROPERTY_ENABLED = "enabled";

    private static final Set<String> SUBSCRIBED_EVENT_TYPES = Set.of(ItemStateEvent.TYPE, ItemCommandEvent.TYPE);

    /**
     * The refresh interval is used to check if any of the bound items is expired.
     * One second should be fine for all use cases, so it's final and cannot be configured.
     */
    private static final long refreshInterval = 1000;

    private final Logger logger = LoggerFactory.getLogger(ExpireManager.class);

    private Map<String, ExpireConfig> itemExpireConfig = new ConcurrentHashMap<>();
    private Map<String, Instant> itemExpireMap = new ConcurrentHashMap<>();

    private ScheduledExecutorService threadPool = ThreadPoolManager
            .getScheduledPool(ThreadPoolManager.THREAD_POOL_NAME_COMMON);

    private final EventPublisher eventPublisher;
    private final MetadataRegistry metadataRegistry;

    private boolean enabled = true;

    private @Nullable ScheduledFuture<?> expireJob;

    @Activate
    public ExpireManager(Map<String, @Nullable Object> configuration, final @Reference EventPublisher eventPublisher,
            final @Reference MetadataRegistry metadataRegistry) {
        this.eventPublisher = eventPublisher;
        this.metadataRegistry = metadataRegistry;

        modified(configuration);
    }

    @Modified
    protected void modified(Map<String, @Nullable Object> configuration) {
        Object valueEnabled = configuration.get(PROPERTY_ENABLED);
        if (valueEnabled != null) {
            enabled = Boolean.parseBoolean(valueEnabled.toString());
        }
        if (enabled) {
            if (expireJob == null) {
                expireJob = threadPool.schedule(() -> {
                    if (!itemExpireMap.isEmpty()) {
                        for (String itemName : itemExpireConfig.keySet()) {
                            if (isReadyToExpire(itemName)) {
                                expire(itemName);
                            }
                        }
                    }
                }, 1, TimeUnit.SECONDS);
            }
        } else {
            if (expireJob != null) {
                expireJob.cancel(true);
                expireJob = null;
            }
        }
    }

    public void receiveCommand(ItemCommandEvent commandEvent) {
        Command command = commandEvent.getItemCommand();
        String itemName = commandEvent.getItemName();
        logger.trace("Received command '{}' for item {}", command, itemName);
        ExpireConfig expireConfig = itemExpireConfig.get(itemName);
        if (expireConfig != null) {
            Command expireCommand = expireConfig.expireCommand;
            State expireState = expireConfig.expireState;

            if ((expireCommand != null && expireCommand.equals(command))
                    || (expireState != null && expireState.equals(command))) {
                // New command is expired command or state -> no further action needed
                itemExpireMap.remove(itemName); // remove expire trigger until next update or command
                logger.debug("Item {} received command '{}'; stopping any future expiration.", itemName, command);
            } else {
                // New command is not the expired command or state, so add the trigger to the map
                Duration duration = expireConfig.duration;
                itemExpireMap.put(itemName, Instant.now().plus(duration));
                logger.debug("Item {} will expire (with '{}' {}) in {} ms", itemName,
                        expireCommand == null ? expireState : expireCommand,
                        expireCommand == null ? "state" : "command", duration);
            }
        }
    }

    protected void receiveUpdate(ItemStateEvent stateEvent) {
        State state = stateEvent.getItemState();
        String itemName = stateEvent.getItemName();
        logger.trace("Received update '{}' for item {}", state, itemName);
        ExpireConfig expireConfig = itemExpireConfig.get(itemName);
        if (expireConfig != null) {
            Command expireCommand = expireConfig.expireCommand;
            State expireState = expireConfig.expireState;

            if ((expireCommand != null && expireCommand.equals(state))
                    || (expireState != null && expireState.equals(state))) {
                // New state is expired command or state -> no further action needed
                itemExpireMap.remove(itemName); // remove expire trigger until next update or command
                logger.debug("Item {} received update '{}'; stopping any future expiration.", itemName, state);
            } else {
                // New state is not the expired command or state, so add the trigger to the map
                Duration duration = expireConfig.duration;
                itemExpireMap.put(itemName, Instant.now().plus(duration));
                logger.debug("Item {} will expire (with '{}' {}) in {} ms", itemName,
                        expireCommand == null ? expireState : expireCommand,
                        expireCommand == null ? "state" : "command", duration);
            }
        }
    }

    private void postCommand(String itemName, Command command) {
        eventPublisher.post(ItemEventFactory.createCommandEvent(itemName, command, EVENT_SOURCE));
    }

    private void postUpdate(String itemName, State state) {
        eventPublisher.post(ItemEventFactory.createStateEvent(itemName, state, EVENT_SOURCE));
    }

    private boolean isAcceptedState(State newState, Item item) {
        boolean isAccepted = false;
        if (item.getAcceptedDataTypes().contains(newState.getClass())) {
            isAccepted = true;
        } else {
            // Look for class hierarchy
            for (Class<?> state : item.getAcceptedDataTypes()) {
                try {
                    if (!state.isEnum() && state.newInstance().getClass().isAssignableFrom(newState.getClass())) {
                        isAccepted = true;
                        break;
                    }
                } catch (InstantiationException e) {
                    logger.warn("InstantiationException on {}", e.getMessage(), e); // Should never happen
                } catch (IllegalAccessException e) {
                    logger.warn("IllegalAccessException on {}", e.getMessage(), e); // Should never happen
                }
            }
        }
        return isAccepted;
    }

    private boolean isReadyToExpire(String itemName) {
        Instant nextExpiry = itemExpireMap.get(itemName);
        return (nextExpiry != null) && Instant.now().isAfter(nextExpiry);
    }

    private void expire(String itemName) {
        itemExpireMap.remove(itemName); // disable expire trigger until next update or command
        ExpireConfig expireConfig = itemExpireConfig.get(itemName);

        if (expireConfig != null) {
            Command expireCommand = expireConfig.expireCommand;
            State expireState = expireConfig.expireState;

            if (expireCommand != null) {
                logger.debug("Item {} received no command or update for {} - posting command '{}'", itemName,
                        expireConfig.duration, expireCommand);
                postCommand(itemName, expireCommand);
            } else if (expireState != null) {
                logger.debug("Item {} received no command or update for {} - posting state '{}'", itemName,
                        expireConfig.duration, expireState);
                postUpdate(itemName, expireState);
            }
        }
    }

    @Override
    public Set<String> getSubscribedEventTypes() {
        return SUBSCRIBED_EVENT_TYPES;
    }

    @Override
    public @Nullable EventFilter getEventFilter() {
        return null;
    }

    @Override
    public void receive(Event event) {
        if (event instanceof ItemStateEvent) {
            receiveUpdate((ItemStateEvent) event);
        } else if (event instanceof ItemCommandEvent) {
            receiveCommand((ItemCommandEvent) event);
        }
    }

    private class ExpireConfig {

        public ExpireConfig(String configString) {
            duration = Duration.ofMinutes(1);
            expireCommand = null;
            expireState = null;
        }

        @Nullable
        final Command expireCommand;
        @Nullable
        final State expireState;
        final Duration duration;
    }
}
