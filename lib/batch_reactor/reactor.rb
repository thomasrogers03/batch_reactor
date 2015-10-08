module BatchReactor
  class Reactor
    include MonitorMixin

    def initialize(options, &yield_batch_callback)
      @yield_batch_callback = yield_batch_callback
      @front_buffer = []
      @back_buffer = []
      @stopped_promise = Ione::Promise.new
      @max_batch_size = options.fetch(:max_batch_size) { 100 }
      @work_wait_time = options.fetch(:work_wait_time) { 0.1 }
      super()
    end

    def start
      promise = Ione::Promise.new
      Thread.start do
        promise.fulfill(self)

        until @stopping
          swap_buffers if @front_buffer.empty?

          if @front_buffer.empty?
            sleep @work_wait_time
            next
          end

          process_batch
        end

        @stopped_promise.fulfill(self)

        # TODO: somehow test this (very hard, since it's a race condition)
        swap_buffers
        process_batch
      end
      promise.future
    end

    def stop
      @stopping = true
      @stopped_promise.future
    end

    def perform_within_batch(&block)
      promise = Ione::Promise.new
      if @stopped_promise.future.resolved?
        promise.fail(StandardError.new('Reactor stopped!'))
      else
        synchronize { @back_buffer << Work.new(block, promise) }
      end
      promise.future
    end

    private

    Work = Struct.new(:proc, :promise, :result)

    def swap_buffers
      synchronize do
        temp_buffer = @front_buffer
        @front_buffer = @back_buffer
        @back_buffer = temp_buffer
      end
    end

    def process_batch
      buffer = @front_buffer.slice!(0...@max_batch_size)
      batch_future = @yield_batch_callback.call do |batch|
        buffer.each do |work|
          work.result = work.proc.call(batch)
        end
      end
      batch_future.on_value do
        buffer.each { |work| work.promise.fulfill(work.result) }
      end
      batch_future.on_failure do |error|
        buffer.each { |work| work.promise.fail(error) }
      end
    end

  end
end
