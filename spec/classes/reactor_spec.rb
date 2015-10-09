require 'rspec'

module BatchReactor
  describe Reactor do

    MockBatch = Struct.new(:results)

    let(:result_batches) { [] }
    let(:batch) { MockBatch.new }
    let(:batch_value) { batch.results }
    let(:batch_error) { nil }
    let(:batch_proc) do
      ->(&block) do
        promise = Ione::Promise.new
        batch.results = []
        block.call(batch.results)
        result_batches << batch.results
        if batch_value
          promise.fulfill(batch_value)
        else
          promise.fail(batch_error)
        end
        promise.future
      end
    end
    let(:options) { {} }

    subject { Reactor.new(options, &batch_proc) }

    describe 'processing thread' do
      it 'should sleep for 100 ms when there is no work' do
        expect(subject).to receive(:sleep).with(0.1).at_least :once
        subject.start.get.stop.get
      end

      context 'with different options' do
        let(:options) { {no_work_backoff: 1} }

        it 'should allow the sleep time to be overridden' do
          expect(subject).to receive(:sleep).with(1).at_least :once
          subject.start.get.stop.get
        end
      end
    end

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

      context 'when called twice' do
        it 'should only start the reactor thread once' do
          expect(Thread).to receive(:start).once.and_call_original
          subject.start.get
          subject.start.get
        end
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

      describe 'clean shutdown' do
        context 'when there is remaining work in the processing buffer' do
          let(:options) { {max_batch_size: 1} }
          let(:state) { {clean_shutdown: false} }
          let(:extra_batch_count) { 0 }

          before do
            waiting = true
            state[:clean_shutdown] = false

            started = false
            subject.perform_within_batch { started = true; sleep 0.1 while waiting }
            extra_batch_count.times { subject.perform_within_batch {} }
            subject.perform_within_batch { state[:clean_shutdown] = true }
            sleep 0.1 until started
            stopped_future = subject.stop
            #noinspection RubyUnusedLocalVariable
            waiting = false
            stopped_future.get
          end

          it 'should finish processing everything in that batch' do
            expect(state[:clean_shutdown]).to eq(true)
          end

          context 'with multiple batches left' do
            it 'should finish processing everything in that batch' do
              expect(state[:clean_shutdown]).to eq(true)
            end
          end

        end
      end
    end

    describe '#perform_within_batch' do
      before { subject.start.get }
      after { subject.stop.get }

      it 'should return an Ione::Future' do
        expect(subject.perform_within_batch {}).to be_a_kind_of(Ione::Future)
      end

      it 'should yield the batch to the provided block' do
        subject.perform_within_batch { |batch| batch << :item }.get
        expect(batch.results).to eq([:item])
      end

      context 'with multiple executions' do
        it 'should call the block for each execution' do
          Ione::Future.all(
              subject.perform_within_batch { |batch| batch << :item_one },
              subject.perform_within_batch { |batch| batch << :item_two }
          ).get
          expect(batch.results).to eq([:item_one, :item_two])
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

      context 'with many items enqueued' do
        it 'should distribute no more than 100 items across multiple batches' do
          futures = 1000.times.map { subject.perform_within_batch { |batch| batch << :item } }
          Ione::Future.all(futures).get
          subject.stop.get
          big_batch = result_batches.find { |batch| batch.size > 100 }
          expect(big_batch).to be_nil
        end

        context 'with a different batch size' do
          let(:options) { {max_batch_size: 10} }

          it 'should distribute no more than the specified number of items across multiple batches' do
            futures = 100.times.map { subject.perform_within_batch { |batch| batch << :item } }
            Ione::Future.all(futures).get
            subject.stop.get
            big_batch = result_batches.find { |batch| batch.size > 10 }
            expect(big_batch).to be_nil
          end
        end
      end
    end

  end
end
