require 'rspec'

module BatchReactor
  describe Reactor do

    let(:results) { [] }
    let(:batch_proc) do
      ->(&block) do
        promise = Ione::Promise.new
        results.clear
        block.call(results)
        promise.fulfill(results)
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

    describe '#perform_with_batch_async' do
      before { subject.start.get }

      it 'should return an Ione::Future' do
        expect(subject.perform_with_batch_async {}).to be_a_kind_of(Ione::Future)
      end

      it 'should yield the batch to the provided block' do
        subject.perform_with_batch_async { |batch| batch << :item }.get
        expect(results).to eq([:item])
      end

      context 'with multiple executions' do
        it 'should call the block for each execution' do
          Ione::Future.all(
              subject.perform_with_batch_async { |batch| batch << :item_one },
              subject.perform_with_batch_async { |batch| batch << :item_two }
          ).get
          expect(results).to eq([:item_one, :item_two])
        end
      end
    end

  end
end
