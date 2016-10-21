require 'rspec'

module BatchReactor
  module Retry
    module Policy
      describe Always do

        it { is_expected.to be_a_kind_of(Policy) }

        it { is_expected.to respond_to(:should_retry?).with(2).arguments }

        it 'should always retry' do
          expect(subject.should_retry?(nil, nil)).to eq(true)
        end

        describe '#count_retry' do
          let(:retry_count) { rand(5..10) }

          it { is_expected.to respond_to(:count_retry).with(1).arguments }

          it 'should keep a count of the number of failed batches' do
            retry_count.times { subject.count_retry(nil) }
            expect(subject.retry_count).to eq(retry_count)
          end
        end

      end
    end
  end
end
