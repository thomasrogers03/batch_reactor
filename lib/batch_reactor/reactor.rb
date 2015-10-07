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

          @yield_batch_callback.call do |batch|
            @front_buffer.each do |work|
              work.proc.call(batch)
              work.promise.fulfill(nil)
            end
          end.get
          @front_buffer.clear
        end
      end
      promise.future
    end

    def perform_with_batch_async(&block)
      promise = Ione::Promise.new
      synchronize do
        @back_buffer << Work.new(block, promise)
      end
      promise.future
    end

    private

    Work = Struct.new(:proc, :promise)

    def swap_buffers
      synchronize do
        temp_buffer = @front_buffer
        @front_buffer = @back_buffer
        @back_buffer = temp_buffer
      end
    end

  end
end
