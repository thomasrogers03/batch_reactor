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

      end
    end
  end
end
