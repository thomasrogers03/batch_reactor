require 'rspec'

module BatchReactor
  describe Reactor do

    let(:results) { [] }
    let(:batch_value) { results }
    let(:batch_error) { nil }
    let(:batch_proc) do
      ->(&block) do
        promise = Ione::Promise.new
        results.clear
        block.call(results)
        if batch_value
          promise.fulfill(batch_value)
        else
          promise.fail(batch_error)
        end
        promise.future
      end
    end

    subject { Reactor.new(&batch_proc) }

    describe '#start' do
      it 'should return an Ione::Future' do
        expect(subject.start).to be_a_kind_of(Ione::Future)
      end

      it 'should resolve to the reactor instance' do
        expect(subject.start.get).to eq(subject)
      end

      it 'should not resolve until the start thread is running' do
        allow(Thread).to receive(:start)
        expect(subject.start.resolved?).to eq(false)
      end
    end

    describe '#stop' do
      before { subject.start.get }

      it 'should return an Ione::Future' do
        expect(subject.stop).to be_a_kind_of(Ione::Future)
      end

      it 'should resolve to the reactor instance' do
        expect(subject.stop.get).to eq(subject)
      end

      it 'should raise an error when someone attempts to process something' do
        subject.stop.get
        future = subject.perform_within_batch { |batch| batch << :item }
        expect { future.get }.to raise_error(StandardError, 'Reactor stopped!')
      end
    end

    describe '#perform_within_batch' do
      before { subject.start.get }

      it 'should return an Ione::Future' do
        expect(subject.perform_within_batch {}).to be_a_kind_of(Ione::Future)
      end

      it 'should yield the batch to the provided block' do
        subject.perform_within_batch { |batch| batch << :item }.get
        expect(results).to eq([:item])
      end

      context 'with multiple executions' do
        it 'should call the block for each execution' do
          Ione::Future.all(
              subject.perform_within_batch { |batch| batch << :item_one },
              subject.perform_within_batch { |batch| batch << :item_two }
          ).get
          expect(results).to eq([:item_one, :item_two])
        end
      end

      it 'should return a future resolving to the result of the block' do
        future = subject.perform_within_batch { |batch| batch << :item; :result }
        expect(future.get).to eq(:result)
      end

      context 'when the batch fails' do
        let(:batch_value) { nil }
        let(:batch_error) { StandardError.new('Batch failed!') }

        it 'should return a future resolving to the result of the block' do
          future = subject.perform_within_batch { |batch| batch << :item; :result }
          expect { future.get }.to raise_error(StandardError, 'Batch failed!')
        end
      end
    end

  end
end
