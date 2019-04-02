/*
 * Copyright 2019 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wraps a {@link Future} and provides a {@link CompletionStage} implementation on top of it.
 *
 * @param <V> the value type.
 */
final class CompletionStageAdapter<V> implements CompletionStage<V> {
    private static final Object MARKER = new Object();
    private static final Object ERROR_MARKER = new Object();

    private final Future<V> future;

    CompletionStageAdapter(Future<V> future) {
        this.future = future;
    }

    private EventExecutor executor() {
        return future.executor();
    }

    @Override
    public <U> CompletionStage<U> thenApply(Function<? super V, ? extends U> fn) {
        return thenApplyAsync(fn);
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return thenApplyAsync(fn, executor());
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        Promise<U> promise = executor().newPromise();
        future.addListener(future -> {
            Throwable cause = future.cause();
            if (cause == null) {
                @SuppressWarnings("unchecked") V value = (V) future.getNow();
                if (inEventLoop(executor)) {
                    thenApplyAsync0(promise, value, fn);
                } else {
                    safeExecute(executor, () -> thenApplyAsync0(promise, value, fn), promise);
                }
            } else {
                promise.setFailure(cause);
            }
        });
        return promise.asStage();
    }

    private static <U, V> void thenApplyAsync0(Promise<U> promise, V value, Function<? super V, ? extends U> fn) {
        final U result;
        try {
            result = fn.apply(value);
        } catch (Throwable cause) {
            promise.setFailure(cause);
            return;
        }
        promise.setSuccess(result);
    }

    @Override
    public CompletionStage<Void> thenAccept(Consumer<? super V> action) {
        return thenAcceptAsync(action);
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action) {
        return thenAcceptAsync(action, executor());
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        Promise<Void> promise = executor().newPromise();
        future.addListener(future -> {
            Throwable cause = future.cause();
            if (cause == null) {
                @SuppressWarnings("unchecked") V value = (V) future.getNow();
                if (inEventLoop(executor)) {
                    thenAcceptAsync0(promise, value, action);
                } else {
                    safeExecute(executor, () -> thenAcceptAsync0(promise, value, action), promise);
                }
            } else {
                promise.setFailure(cause);
            }
        });
        return promise.asStage();
    }

    private static <U, V> void thenAcceptAsync0(Promise<U> promise, V value, Consumer<? super V> action) {
        try {
            action.accept(value);
            promise.setSuccess(null);
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
    }

    @Override
    public CompletionStage<Void> thenRun(Runnable action) {
        return thenRunAsync(action);
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action) {
        return thenRunAsync(action, executor());
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
        return thenAcceptAsync(ignore -> action.run(), executor);
    }

    @Override
    public <U, V1> CompletionStage<V1> thenCombine(
            CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn) {
        return thenCombineAsync(other, fn);
    }

    @Override
    public <U, V1> CompletionStage<V1> thenCombineAsync(
            CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn) {
        return thenCombineAsync(other, fn, executor());
    }

    @Override
    public <U, V1> CompletionStage<V1> thenCombineAsync(
            CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        Promise<V1> promise = executor().newPromise();
        AtomicReference<Object> reference = new AtomicReference<>(MARKER);
        whenCompleteAsync((v, error) -> {
            if (error != null) {
                reference.set(ERROR_MARKER);
                promise.setFailure(error);
            } else if (!reference.compareAndSet(MARKER, v)) {
                Object rawValue = reference.get();
                if (rawValue == ERROR_MARKER) {
                    assert promise.isDone();
                    return;
                }
                @SuppressWarnings("unchecked") U value = (U) rawValue;

                final V1 result;
                try {
                    result = fn.apply(v, value);
                } catch (Throwable cause) {
                    promise.setFailure(cause);
                    return;
                }
                promise.setSuccess(result);
            }

        }, executor);
        other.whenCompleteAsync((v, error) -> {
            if (error != null) {
                reference.set(ERROR_MARKER);
                promise.setFailure(error);
            } else if (!reference.compareAndSet(MARKER, v)) {
                Object rawValue = reference.get();
                if (rawValue == ERROR_MARKER) {
                    assert promise.isDone();
                    return;
                }

                @SuppressWarnings("unchecked") V value = (V) rawValue;
                final V1 result;
                try {
                    result = fn.apply(value, v);
                } catch (Throwable cause) {
                    promise.setFailure(cause);
                    return;
                }
                promise.setSuccess(result);
            }
        }, executor);
        return promise.asStage();
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBoth(
            CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return thenAcceptBothAsync(other, action);
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(
            CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return thenAcceptBothAsync(other, action, executor());
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(
            CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action, Executor executor) {
        return thenCombineAsync(other, (value, error) -> {
            action.accept(value, error);
            return null;
        }, executor);
    }

    @Override
    public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return runAfterBothAsync(other, action);
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return runAfterBothAsync(other, action, executor());
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return thenCombineAsync(other, (ignoreValue, ignoreError) -> {
            action.run();
            return null;
        }, executor);
    }

    @Override
    public <U> CompletionStage<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return applyToEitherAsync(other, fn);
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return applyToEitherAsync(other, fn, executor());
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(
            CompletionStage<? extends V> other, Function<? super V, U> fn, Executor executor) {
        Promise<U> promise = executor().newPromise();

        class AtomicBiConsumer extends AtomicReference<Object> implements BiConsumer<V, Throwable> {

            AtomicBiConsumer() {
                super(MARKER);
            }

            @Override
            public void accept(V v, Throwable error) {
                if (error != null) {
                    set(ERROR_MARKER);
                    promise.tryFailure(error);
                } else if (compareAndSet(MARKER, v)) {
                    final U value;
                    try {
                        value = fn.apply(v);
                    } catch (Throwable cause) {
                        promise.setFailure(cause);
                        return;
                    }
                    promise.setSuccess(value);
                }
            }
        }

        BiConsumer<V, Throwable> consumer = new AtomicBiConsumer();
        whenCompleteAsync(consumer, executor);
        other.whenCompleteAsync(consumer, executor);
        return promise.asStage();
    }

    @Override
    public CompletionStage<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(other, action);
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(other, action, executor());
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(
            CompletionStage<? extends V> other, Consumer<? super V> action, Executor executor) {
        Promise<Void> promise = executor().newPromise();

        class AtomicBiConsumer extends AtomicReference<Object> implements BiConsumer<V, Throwable> {

            AtomicBiConsumer() {
                super(MARKER);
            }

            @Override
            public void accept(V v, Throwable cause) {
                if (cause == null) {
                    if (compareAndSet(MARKER, v)) {
                        try {
                            action.accept(v);
                        } catch (Throwable error) {
                            promise.setFailure(error);
                            return;
                        }
                        promise.setSuccess(null);
                    }
                } else if (compareAndSet(MARKER, cause)) {
                    promise.setFailure(cause);
                }
            }
        }

        BiConsumer<? super V, Throwable> consumer = new AtomicBiConsumer();
        whenCompleteAsync(consumer, executor);
        other.whenCompleteAsync(consumer, executor);
        return promise.asStage();
    }

    @Override
    public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return runAfterEitherAsync(other, action);
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return runAfterEitherAsync(other, action, future.executor());
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        Promise<Void> promise = executor().newPromise();

        class AtomicBiConsumer extends AtomicReference<Object> implements BiConsumer<Object, Throwable> {

            AtomicBiConsumer() {
                super(MARKER);
            }

            @Override
            public void accept(Object v, Throwable cause) {
                if (cause == null) {
                    if (compareAndSet(MARKER, v)) {
                        try {
                            action.run();
                        } catch (Throwable error) {
                            promise.setFailure(error);
                            return;
                        }
                        promise.setSuccess(null);
                    }
                } else if (compareAndSet(MARKER, cause)) {
                    promise.setFailure(cause);
                }
            }
        }

        BiConsumer<Object, Throwable> consumer = new AtomicBiConsumer();
        whenCompleteAsync(consumer, executor);
        other.whenCompleteAsync(consumer, executor);
        return promise.asStage();
    }

    @Override
    public <U> CompletionStage<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn);
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn, executor());
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(
            Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        Promise<U> promise = executor().newPromise();
        future.addListener(f -> {
           Throwable cause = f.cause();
           if (cause == null) {
               @SuppressWarnings("unchecked") V value = (V) f.getNow();
               if (inEventLoop(executor)) {
                   thenComposeAsync0(promise, fn, value);
               } else {
                   safeExecute(executor, () -> thenComposeAsync0(promise, fn, value), promise);
               }
           } else {
               promise.setFailure(cause);
           }
        });
        return promise.asStage();
    }

    private static <V, U> void thenComposeAsync0(
            Promise<U> promise, Function<? super V, ? extends CompletionStage<U>> fn, V value) {
        fn.apply(value).whenComplete((v, error) -> {
            if (error != null) {
                promise.setFailure(error);
            } else {
                promise.setSuccess(v);
            }
        });
    }

    @Override
    public CompletionStage<V> exceptionally(Function<Throwable, ? extends V> fn) {
        Promise<V> promise = executor().newPromise();
        future.addListener(f -> {
            Throwable error = f.cause();
            if (error != null) {
                final V result;
                try {
                    result = fn.apply(error);
                } catch (Throwable cause) {
                    promise.setFailure(cause);
                    return;
                }
                promise.setSuccess(result);
            } else {
                @SuppressWarnings("unchecked") V value = (V) f.getNow();
                promise.setSuccess(value);
            }
        });
        return promise.asStage();
    }

    @Override
    public CompletionStage<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return whenCompleteAsync(action);
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return whenCompleteAsync(action, executor());
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        Promise<V> promise = executor().newPromise();
        future.addListener(f -> {
            if (inEventLoop(executor)) {
                whenCompleteAsync0(promise, f, action);
            } else {
                safeExecute(executor, () -> whenCompleteAsync0(promise, f, action), promise);
            }
        });
        return promise.asStage();
    }

    @SuppressWarnings("unchecked")
    private static <U, V> void whenCompleteAsync0(
            Promise<U> promise, Future<? super V> f, BiConsumer<? super V, ? super Throwable> action) {
        try {
            action.accept((V) f.getNow(), f.cause());
        } catch (Throwable cause) {
            promise.setFailure(cause);
            return;
        }
        promise.setSuccess(null);
    }

    @Override
    public <U> CompletionStage<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return handleAsync(fn);
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return handleAsync(fn, executor());
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        Promise<U> promise = executor().newPromise();
        future.addListener(f -> {
            if (inEventLoop(executor)) {
                handleAsync0(promise, f, fn);
            } else {
                safeExecute(executor, () -> handleAsync0(promise, f, fn), promise);
            }
        });
        return promise.asStage();
    }

    @SuppressWarnings("unchecked")
    private static <U, V> void handleAsync0(
            Promise<U> promise, Future<? super V> f, BiFunction<? super V, Throwable, ? extends U> fn) {
        final U result;
        try {
            result = fn.apply((V) f.getNow(), f.cause());
        } catch (Throwable cause) {
            promise.setFailure(cause);
            return;
        }
        promise.setSuccess(result);
    }

    private static boolean inEventLoop(Executor executor) {
        return executor instanceof EventExecutor && ((EventExecutor) executor).inEventLoop();
    }

    private static void safeExecute(Executor executor, Runnable task, Promise<?> promise) {
        try {
            executor.execute(task);
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        throw new UnsupportedOperationException();
    }
}
