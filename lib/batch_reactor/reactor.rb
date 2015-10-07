module BatchReactor
  class Reactor
    include MonitorMixin

    def initialize(&yield_batch_callback)
      @yield_batch_callback = yield_batch_callback
      @front_buffer = []
      @back_buffer = []
      super
    end

    def start
      promise = Ione::Promise.new
      Thread.start do
        promise.fulfill(self)

        loop do
          swap_buffers

          if @front_buffer.empty?
            sleep 0.1
            next
          end

          buffer = @front_buffer
          @front_buffer = []
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
      promise.future
    end

    def perform_within_batch(&block)
      promise = Ione::Promise.new
      synchronize do
        @back_buffer << Work.new(block, promise)
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

  end
end
