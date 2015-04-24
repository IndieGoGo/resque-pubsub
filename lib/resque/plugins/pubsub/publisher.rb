module Resque
  module Plugins
    module Pubsub
      module Publisher

        def self.included(base)
          base.send(:include, InstanceMethods)
        end

        module InstanceMethods

          def publish(topic, message)
            Resque.logger.info "[#{self.class.to_s}] publishing #{message} in #{topic}"
            Exchange.redis.sadd(:queues, 'messages')
            Exchange.redis.rpush('queue:messages', { :class => 'Resque::Plugins::Pubsub::Broker', :args => [topic, message] }.to_json)
          end

        end

      end
    end
  end
end
