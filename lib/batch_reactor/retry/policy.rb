module BatchReactor
  module Retry
    module Policy
      class BatchRetriedError < StandardError
      end

      ERROR = BatchRetriedError.new

      def handle_future(lock, future, batch, retry_buffer)
        future.fallback do |error|
          work_by_status = work_by_status(batch, error)
          fail_failures!(error, work_by_status[false])
          perform_retries!(lock, retry_buffer, work_by_status[true])
          count_retry(error)
          ThomasUtils::Future.error(ERROR)
        end
      end

      def should_throttle?
        false
      end

      private

      def work_by_status(batch, error)
        batch.group_by do |work|
          !!should_retry?(work, error)
        end
      end

      def perform_retries!(lock, retry_buffer, successful_results)
        if successful_results
          lock.synchronize do
            successful_results.each do |work|
              work.retry_count += 1
              retry_buffer << work
            end
          end
        end
      end

      def fail_failures!(error, failed_results)
        if failed_results
          failed_results.each do |work|
            work.promise.fail(error)
          end
        end
      end
    end
  end
end
