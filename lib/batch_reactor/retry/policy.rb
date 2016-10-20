module BatchReactor
  module Policy
    class BatchRetriedError < StandardError
    end

    ERROR = BatchRetriedError.new

    def handle_future(lock, future, batch, retry_buffer)
      future.fallback do |error|
        work_by_status = batch.group_by do |work|
          !!should_retry?(work, error)
        end
        failed_results = work_by_status[false]
        if failed_results
          failed_results.each do |work|
            work.promise.fail(error)
          end
        end
        successful_results = work_by_status[true]
        if successful_results
          lock.synchronize do
            successful_results.each do |work|
              work.retry_count += 1
              retry_buffer << work
            end
          end
        end
        ThomasUtils::Future.error(ERROR)
      end
    end
  end
end
