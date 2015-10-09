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

        until @stopping
          swap_buffers if needs_work?
          next if no_work?
          process_batch
        end

        shutdown
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
      batch_future.on_value { handle_success(buffer) }
      batch_future.on_failure { |error| handle_failure(buffer, error) }
    end

    def create_batch(buffer)
      @yield_batch_callback.call { |batch| perform_work(batch, buffer) }
    end

    def perform_work(batch, buffer)
      buffer.each { |work| work.result = work.proc.call(batch) }
    end

    def handle_success(buffer)
      buffer.each { |work| work.promise.fulfill(work.result) }
    end

    def handle_failure(buffer, error)
      buffer.each { |work| work.promise.fail(error) }
    end

    def shutdown
      @stopping_promise.fulfill(self)

      # TODO: somehow test this (very hard, since it's a race condition)
      swap_buffers
      process_batch

      @stopped_promise.fulfill(self)
    end

  end
end
