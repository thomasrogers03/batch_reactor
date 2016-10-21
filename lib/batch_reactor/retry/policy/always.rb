module BatchReactor
  module Retry
    module Policy
      class Always
        include Policy

        def should_retry?(_, _)
          true
        end

        def count_retry(_)

        end
      end
    end
  end
end
