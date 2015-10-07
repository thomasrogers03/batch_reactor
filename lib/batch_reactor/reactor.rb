module BatchReactor
  class Reactor

    def initialize(&yield_batch_callback)
      @yield_batch_callback = yield_batch_callback
      @queue = Queue.new
    end

    def start
      promise = Ione::Promise.new
      Thread.start do
        promise.fulfill(self)

        loop do
          first_item = @queue.pop
          item_count = @queue.size

          items = [first_item] + item_count.times.map { @queue.pop }
          @yield_batch_callback.call do |batch|
            items.each do |work|
              work.proc.call(batch)
              work.promise.fulfill(nil)
            end
          end

        end
      end
      promise.future
    end

    def perform_with_batch_async(&block)
      promise = Ione::Promise.new
      @queue << Work.new(block, promise)
      promise.future
    end

    private

    Work = Struct.new(:proc, :promise)

  end
end
