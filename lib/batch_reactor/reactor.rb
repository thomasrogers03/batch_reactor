module BatchReactor
  class Reactor
    include MonitorMixin
    extend Forwardable

    def initialize(options, &yield_batch_callback)
      @yield_batch_callback = yield_batch_callback
      @front_buffer = []
      @back_buffer = []
      @stopping_promise = Concurrent::IVar.new
      @stopped_promise = Concurrent::IVar.new
      @max_batch_size = options.fetch(:max_batch_size) { 100 }
      @no_work_backoff = options.fetch(:no_work_backoff) { 0.1 }
      super()
    end

    def start
      return make_future(@started_promise) if @started_promise

      @started_promise = Concurrent::IVar.new
      Thread.start do
        @started_promise.set(self)

        last_batch_future = ThomasUtils::Future.none
        until @stopping
          swap_buffers if needs_work?
          next if no_work?
          last_batch_future = process_batch
        end

        last_batch_future.on_complete { |_, _| shutdown }
      end
      make_future(@started_promise)
    end

    def stop
      @stopping = true
      make_future(@stopped_promise)
    end

    def perform_within_batch(&block)
      promise = Concurrent::IVar.new
      if @stopping_promise.fulfilled?
        promise.fail(StandardError.new('Reactor stopped!'))
      else
        synchronize { @back_buffer << Work.new(block, promise) }
      end
      make_future(promise)
    end

    private

    def_delegator :@front_buffer, :empty?, :needs_work?

    Work = Struct.new(:proc, :promise, :result)

    def swap_buffers
      synchronize do
        temp_buffer = @front_buffer
        @front_buffer = @back_buffer
        @back_buffer = temp_buffer
      end
    end

    def no_work?
      if @front_buffer.empty?
        sleep @no_work_backoff
        true
      end
    end

    def process_batch
      buffer = @front_buffer.slice!(0...@max_batch_size)
      batch_future = create_batch(buffer)
      batch_future.on_complete do |_, error|
        error ? handle_failure(buffer, error) : handle_success(buffer)
      end
      batch_future
    end

    def create_batch(buffer)
      @yield_batch_callback.call { |batch| perform_work(batch, buffer) }
    end

    def perform_work(batch, buffer)
      buffer.each { |work| work.result = work.proc.call(batch) }
    end

    def handle_success(buffer)
      buffer.each do |work|
        result = work.result
        if result.respond_to?(:on_complete)
          handle_result_future(result, work)
        else
          work.promise.set(result)
        end
      end
    end

    def handle_result_future(result, work)
      result.on_complete do |value, error|
        error ? work.promise.fail(error) : work.promise.set(value)
      end
    end

    def handle_failure(buffer, error)
      buffer.each { |work| work.promise.fail(error) }
    end

    def shutdown
      @stopping_promise.set(self)

      futures = []
      finish_remaining_work(futures)
      swap_buffers
      finish_remaining_work(futures)

      if futures.any?
        ThomasUtils::Future.all(futures).on_complete { |_, _| @stopped_promise.set(self) }
      else
        @stopped_promise.set(self)
      end
    end

    def finish_remaining_work(futures)
      futures << process_batch until @front_buffer.empty?
    end

    def make_future(promise)
      ThomasUtils::Observation.new(ThomasUtils::Future::IMMEDIATE_EXECUTOR, promise)
    end

  end
end
