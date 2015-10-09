module BatchReactor
  class ReactorCluster

    def initialize(count, options, &batch_callback)
      @reactors = count.times.map do |index|
        Reactor.new(options) { |&block| batch_callback.call(index, &block) }
      end
    end

    def define_partitioner(&partitioner_callback)
      @partitioner_callback = partitioner_callback
    end

    def start
      futures = @reactors.map(&:start)
      Ione::Future.all(futures)
    end

    def stop
      futures = @reactors.map(&:stop)
      Ione::Future.all(futures)
    end

    def perform_within_batch(key, &block)
      partition = @partitioner_callback.call(key)
      @reactors[partition].perform_within_batch(&block)
    end

  end
end
