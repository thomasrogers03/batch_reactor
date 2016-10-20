require 'rspec'

module BatchReactor
  describe Policy do

    let(:policy_klass) do
      Struct.new(:max_retries, :bad_error) do
        include Policy

        def should_retry?(work, error)
          !(bad_error && error.is_a?(bad_error)) &&
              (max_retries == -1 || work.retry_count < max_retries)
        end
      end
    end
    let(:work_klass) do
      Class.new(Struct.new(:promise, :data, :retry_count)) do
        def initialize
          super(Concurrent::IVar.new, Faker::Lorem.sentence, 0)
        end
      end
    end
    let(:bad_error) { nil }
    let(:max_retries) { -1 }

    subject { policy_klass.new(max_retries, bad_error) }

    describe '#handle_future' do
      let(:lock) { Mutex.new }
      let(:future_value) { Faker::Lorem.sentence }
      let(:future) { ThomasUtils::Future.value(future_value) }
      let(:work) { work_klass.new }
      let(:batch) { [work] }
      let(:retry_buffer) { [] }

      it 'should return a future resolving to the result' do
        expect(subject.handle_future(lock, future, batch, retry_buffer).get).to eq(future_value)
      end

      it 'should do nothing with the buffer' do
        subject.handle_future(lock, future, batch, retry_buffer)
        expect(retry_buffer).to be_empty
      end

      context 'when the future returns an error' do
        let(:error) { Interrupt.new }
        let(:future) { ThomasUtils::Future.error(error) }
        let(:allow_retries) { true }

        it 'should return a future resolving to an error indicated the batch was retried' do
          expect { subject.handle_future(lock, future, batch, retry_buffer).get }.to raise_error(Policy::BatchRetriedError)
        end

        it 'should append the work back into the buffer' do
          subject.handle_future(lock, future, batch, retry_buffer)
          expect(retry_buffer).to include(work)
        end

        it 'should increment the retry count' do
          subject.handle_future(lock, future, batch, retry_buffer)
          expect(work.retry_count).to eq(1)
        end

        context 'with multiple items' do
          let(:work_two) { work_klass.new }
          let(:batch) { [work, work_two] }

          before { work_two.retry_count = 9 }

          it 'should append the first item' do
            subject.handle_future(lock, future, batch, retry_buffer)
            expect(retry_buffer).to include(work)
          end

          it 'should append the second item' do
            subject.handle_future(lock, future, batch, retry_buffer)
            expect(retry_buffer).to include(work_two)
          end

          it 'should increment the retry count for the first item' do
            subject.handle_future(lock, future, batch, retry_buffer)
            expect(work.retry_count).to eq(1)
          end

          it 'should increment the retry count for the second item' do
            subject.handle_future(lock, future, batch, retry_buffer)
            expect(work_two.retry_count).to eq(10)
          end

          context 'when we are no longer able to retry some of this work' do
            let(:max_retries) { 9 }

            it 'should append the first item' do
              subject.handle_future(lock, future, batch, retry_buffer)
              expect(retry_buffer).to include(work)
            end

            it 'should NOT append the second item' do
              subject.handle_future(lock, future, batch, retry_buffer)
              expect(retry_buffer).not_to include(work_two)
            end
          end
        end

        shared_examples_for 'work that is no longer able to retry' do
          it 'should do nothing with the buffer' do
            subject.handle_future(lock, future, batch, retry_buffer)
            expect(retry_buffer).to be_empty
          end

          it 'should fail the work' do
            subject.handle_future(lock, future, batch, retry_buffer)
            expect(work.promise.reason).to eq(error)
          end
        end

        context 'when we are not aloud to retry this error' do
          let(:bad_error) { Interrupt }
          it_behaves_like 'work that is no longer able to retry'
        end

        context 'when we are no longer able to retry this work' do
          let(:max_retries) { 0 }
          it_behaves_like 'work that is no longer able to retry'
        end
      end
    end

  end
end
