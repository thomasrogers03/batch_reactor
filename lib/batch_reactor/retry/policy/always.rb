module BatchReactor
  module Retry
    module Policy
      class Always
        include Policy

        def should_retry?(_, _)
          true
        end
      end
    end
  end
end
