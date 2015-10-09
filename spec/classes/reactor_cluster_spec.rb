require 'rspec'

module BatchReactor
  describe ReactorCluster do

    let(:reactor_count) { 1 }
    let(:batch_callback) { ->(_, &_) {} }
    let(:options) { {} }
    let(:reactor) { mock_reactor }
    let(:reactor_two) { mock_reactor }
    let(:reactors) { [reactor] }

    subject { ReactorCluster.new(reactor_count, options, &batch_callback) }

    before do
      allow(Reactor).to receive(:new).with(options).and_return(*reactors)
    end

    describe '#start' do
      let(:reactor_two) { mock_reactor }

      it 'should start the reactor' do
        expect(reactor).to receive(:start)
        subject.start.get
      end

      it 'should return a future resolving to all created reactors' do
        expect(subject.start.get).to eq(reactors)
      end

      context 'with more reactors' do
        let(:reactor_count) { 2 }
        let(:reactors) { [reactor, reactor_two] }

        it 'should start both reactors' do
          expect(reactor_two).to receive(:start)
          subject.start.get
        end

        it 'should return a future resolving to all created reactors' do
          expect(subject.start.get).to eq(reactors)
        end
      end

      context 'with different options' do
        let(:options) { {max_batch_size: 1} }

        it 'should create the reactor with the specified options' do
          expect(Reactor).to receive(:new).and_return(*reactors)
          subject.start.get
        end
      end

      context 'when called twice' do
        let(:reactors) { [reactor, reactor_two] }

        it 'should re-use the existing reactors' do
          subject.start.get
          expect(subject.start.get).to eq([reactor])
        end
      end
    end

    describe '#stop' do
      before { subject.start.get }

      it 'should stop the reactor' do
        expect(reactor).to receive(:stop)
        subject.stop.get
      end

      it 'should return a future resolving to all created reactors' do
        expect(subject.stop.get).to eq(reactors)
      end

      context 'with more reactors' do
        let(:reactor_count) { 2 }
        let(:reactors) { [reactor, reactor_two] }

        it 'should stop both reactors' do
          expect(reactor_two).to receive(:stop)
          subject.stop.get
        end

        it 'should return a future resolving to all created reactors' do
          expect(subject.stop.get).to eq(reactors)
        end
      end
    end

    describe '#perform_within_batch' do
      let(:reactor_count) { 2 }
      let(:reactors) { [reactor, reactor_two] }
      let(:batches) { [[], []] }
      let(:batch_callback) do
        ->(index, &block) do
          batch = batches[index]
          block.call(batch)
          mock_future(true)
        end
      end
      let(:partitioner) { ->(item) { item % 2 } }

      before do
        reactor_index = 0
        allow(Reactor).to receive(:new) do |_, &callback|
          if reactor_index.zero?
            allow(reactor).to receive(:perform_within_batch) do |&block|
              callback.call(&block)
            end
            reactor_index += 1
            reactor
          else
            allow(reactor_two).to receive(:perform_within_batch) do |&block|
              callback.call(&block)
            end
            reactor_two
          end
        end
        subject.define_partitioner(&partitioner)
      end

      it 'should associate the callback with the batch that lies in the same partition' do
        subject.perform_within_batch(0) { |batch| batch << :item }.get
        expect(batches[0]).to eq([:item])
      end

      context 'with a different key' do
        it 'should associate the callback with the batch that lies in the same partition' do
          subject.perform_within_batch(1) { |batch| batch << :item }.get
          expect(batches[1]).to eq([:item])
        end
      end

      context 'with a different partitioner' do
        let(:partitioner) { ->(item) { (item + 1) % 2 } }

        it 'should associate the callback with the batch that lies in the same partition' do
          subject.perform_within_batch(0) { |batch| batch << :item }.get
          expect(batches[1]).to eq([:item])
        end

        context 'with a different key' do
          it 'should associate the callback with the batch that lies in the same partition' do
            subject.perform_within_batch(1) { |batch| batch << :item }.get
            expect(batches[0]).to eq([:item])
          end
        end
      end
    end

    private

    def mock_reactor
      double(:reactor).tap do |reactor|
        allow(reactor).to receive(:start).and_return(mock_future(reactor))
        allow(reactor).to receive(:stop).and_return(mock_future(reactor))
      end
    end

    def mock_future(value, error = nil)
      promise = Ione::Promise.new
      error ? promise.fail(error) : promise.fulfill(value)
      promise.future
    end

  end
end
