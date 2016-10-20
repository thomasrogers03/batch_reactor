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
        batch.results = []
        block.call(batch.results)
        result_batches << batch.results
        batch_error ? ThomasUtils::Future.error(batch_error) : ThomasUtils::Future.value(batch_value)
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
      it 'should return a ThomasUtils::Observation' do
        expect(subject.start).to be_a_kind_of(ThomasUtils::Observation)
      end

      it 'should resolve to the reactor instance' do
        expect(subject.start.get).to eq(subject)
      end

      it 'should not resolve until the start thread is running' do
        allow(Thread).to receive(:start)
        expect(subject.start.resolved_at).to be_nil
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

      it 'should return a ThomasUtils::Observation' do
        expect(subject.stop).to be_a_kind_of(ThomasUtils::Observation)
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
            let(:extra_batch_count) { 1 }

            it 'should finish processing everything in that batch' do
              expect(state[:clean_shutdown]).to eq(true)
            end
          end

        end

        context 'when remaining work is queued just before shutting down' do
          let(:options) { {max_batch_size: 1} }
          let(:state) { {clean_shutdown: false} }
          let(:extra_batch_count) { 0 }

          before do
            waiting = true
            state[:clean_shutdown] = false

            started = false
            subject.perform_within_batch { started = true; sleep 0.1 while waiting }
            sleep 0.1 until started
            extra_batch_count.times { subject.perform_within_batch {} }
            subject.perform_within_batch { state[:clean_shutdown] = true }
            stopped_future = subject.stop
            #noinspection RubyUnusedLocalVariable
            waiting = false
            stopped_future.get
          end

          it 'should finish processing everything in that batch' do
            expect(state[:clean_shutdown]).to eq(true)
          end

          context 'with multiple batches left' do
            let(:extra_batch_count) { 1 }

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

      it 'should return a ThomasUtils::Observation' do
        expect(subject.perform_within_batch {}).to be_a_kind_of(ThomasUtils::Observation)
      end

      it 'should yield the batch to the provided block' do
        subject.perform_within_batch { |batch| batch << :item }.get
        expect(batch.results).to eq([:item])
      end

      context 'when a maximum buffer size is specified' do
        let(:batch_promise) { Concurrent::IVar.new }
        let(:batch_proc) do
          ->(&block) do
            block.call([])
            ThomasUtils::Observation.new(ThomasUtils::Future::IMMEDIATE_EXECUTOR, batch_promise)
          end
        end
        let(:options) { {max_buffer_size: 1} }

        before do
          subject.perform_within_batch { |batch| batch << :item }
        end

        it 'should return a future resolving to a failure' do
          future = subject.perform_within_batch { |batch| batch << :item }
          expect { future.get }.to raise_error(StandardError, 'Buffer overflow!')
          batch_promise.set('OK')
        end

        context 'when specified to wait, instead of erroring out' do
          let(:options) { {max_buffer_size: 1, buffer_overflow_handler: :wait} }

          it 'should wait until the buffer is ready again' do
            expect(subject).to receive(:sleep).with(0.3) do
              allow(subject).to receive(:sleep)
              batch_promise.set('OK')
              nil
            end
            subject.perform_within_batch { |batch| batch << :item }.get
          end

          it 'should return a future resolving to the result of the block' do
            allow(subject).to receive(:sleep).with(0.3) do
              allow(subject).to receive(:sleep)
              batch_promise.set('OK')
            end
            future = subject.perform_within_batch { |batch| batch << :item; :result }
            expect(future.get).to eq(:result)
          end
        end
      end

      context 'with multiple executions' do
        it 'should call the block for each execution' do
          ThomasUtils::Future.all([
                                      subject.perform_within_batch { |batch| batch << :item_one },
                                      subject.perform_within_batch { |batch| batch << :item_two }
                                  ]).get
          expect(batch.results).to eq([:item_one, :item_two])
        end
      end

      it 'should return a future resolving to the result of the block' do
        future = subject.perform_within_batch { |batch| batch << :item; :result }
        expect(future.get).to eq(:result)
      end

      context 'when the result responds to #on_complete (ie is a future, itself)' do
        let(:result_value) { 5 }
        let(:result_error) { nil }
        let(:result_future) do
          double(:future).tap do |future|
            allow(future).to receive(:on_complete) do |&block|
              block.call(result_value, result_error)
            end
          end
        end

        it 'should return a future resolving to the underlying result' do
          future = subject.perform_within_batch { result_future }
          expect(future.get).to eq(5)
        end

        context 'with a different value' do
          let(:result_value) { 51 }

          it 'should return a future resolving to the underlying result' do
            future = subject.perform_within_batch { result_future }
            expect(future.get).to eq(51)
          end
        end

        context 'with an error' do
          let(:result_error) { StandardError.new('Item in batch failed!') }

          it 'should raise an error on the resolved future' do
            future = subject.perform_within_batch { result_future }
            expect { future.get }.to raise_error(StandardError, 'Item in batch failed!')
          end
        end
      end

      context 'when the batch fails' do
        let(:batch_value) { nil }
        let(:batch_error) { StandardError.new('Batch failed!') }

        it 'should raise an error on the resolved future' do
          future = subject.perform_within_batch { |batch| batch << :item; :result }
          expect { future.get }.to raise_error(StandardError, 'Batch failed!')
        end
      end

      describe 'retry policies' do
        let(:result) { Faker::Lorem.sentence }
        let(:policy_name) { :always }
        let(:policy_klass) { Retry::Policy::Always }
        let(:options) { {retry_policy: policy_name} }

        it 'should return a future resolving to the underlying result' do
          future = subject.perform_within_batch { result }
          expect(future.get).to eq(result)
        end

        context 'with an error' do
          let(:batch_value) { nil }
          let(:batch_error) { StandardError.new(Faker::Lorem.sentence) }

          before do
            allow_any_instance_of(policy_klass).to receive(:handle_future) do |policy, lock, future, batch, retry_buffer|
              expect(lock).to eq(subject)
              expect(future).to be_a_kind_of(ThomasUtils::Observation)
              batch.each { |work| work.promise.set(work.result) }
              expect(retry_buffer).to be_a_kind_of(Array)
              ThomasUtils::Future.error(Retry::Policy::ERROR)
            end
          end

          it 'should let the retry policy handle the failure' do
            future = subject.perform_within_batch { result }
            expect(future.get).to eq(result)
          end
        end
      end

      context 'with many items enqueued' do
        it 'should distribute no more than 100 items across multiple batches' do
          futures = 1000.times.map { subject.perform_within_batch { |batch| batch << :item } }
          ThomasUtils::Future.all(futures).get
          subject.stop.get
          big_batch = result_batches.find { |batch| batch.size > 100 }
          expect(big_batch).to be_nil
        end

        context 'with a different batch size' do
          let(:options) { {max_batch_size: 10} }

          it 'should distribute no more than the specified number of items across multiple batches' do
            futures = 100.times.map { subject.perform_within_batch { |batch| batch << :item } }
            ThomasUtils::Future.all(futures).get
            subject.stop.get
            big_batch = result_batches.find { |batch| batch.size > 10 }
            expect(big_batch).to be_nil
          end
        end
      end
    end

  end
end
