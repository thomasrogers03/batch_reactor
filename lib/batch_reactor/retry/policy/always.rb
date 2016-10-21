module BatchReactor
  module Retry
    module Policy
      class Always
        include Policy
        extend Forwardable

        def_delegator :@retry_count, :value, :retry_count

        def initialize
          @retry_count = Concurrent::AtomicFixnum.new
        end

        def should_retry?(_, _)
          true
        end

        def count_retry(_)
          @retry_count.increment
        end
      end
    end
  end
end
