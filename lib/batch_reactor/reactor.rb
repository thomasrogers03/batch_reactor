module BatchReactor
  class Reactor
    include MonitorMixin
    extend Forwardable

    def initialize(options, &yield_batch_callback)
      @yield_batch_callback = yield_batch_callback
      @front_buffer = []
      @back_buffer = []
      @stopping_promise = Ione::Promise.new
      @stopped_promise = Ione::Promise.new
      @max_batch_size = options.fetch(:max_batch_size) { 100 }
      @no_work_backoff = options.fetch(:no_work_backoff) { 0.1 }
      super()
    end

    def start
      return @started_promise.future if @started_promise

      @started_promise = Ione::Promise.new
      Thread.start do
        @started_promise.fulfill(self)

        last_batch_future = Ione::Future.resolved(nil)
        until @stopping
          swap_buffers if needs_work?
          next if no_work?
          last_batch_future = process_batch
        end

        last_batch_future.on_complete { |_, _| shutdown }
      end
      @started_promise.future
    end

    def stop
      @stopping = true
      @stopped_promise.future
    end

    def perform_within_batch(&block)
      promise = Ione::Promise.new
      if @stopping_promise.future.resolved?
        promise.fail(StandardError.new('Reactor stopped!'))
      else
        synchronize { @back_buffer << Work.new(block, promise) }
      end
      promise.future
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
          work.promise.fulfill(result)
        end
      end
    end

    def handle_result_future(result, work)
      result.on_complete do |value, error|
        error ? work.promise.fail(error) : work.promise.fulfill(value)
      end
    end

    def handle_failure(buffer, error)
      buffer.each { |work| work.promise.fail(error) }
    end

    def shutdown
      @stopping_promise.fulfill(self)

      futures = []
      finish_remaining_work(futures)
      swap_buffers
      finish_remaining_work(futures)

      Ione::Future.all(futures).on_complete { |_, _| @stopped_promise.fulfill(self) }
    end

    def finish_remaining_work(futures)
      futures << process_batch until @front_buffer.empty?
    end

  end
end
