# frozen_string_literal: true

require 'yabeda'
require 'kafka'
require 'yabeda/kafka/version'
require 'yabeda/kafka/proxy'

module Yabeda
  module Kafka
    class << self
      attr_accessor :registry
    end

    Yabeda.configure do
      group :kafka

      Kernel.const_set('PROMETHEUS_NO_AUTO_START', true) unless defined?(PROMETHEUS_NO_AUTO_START)

      require 'kafka/prometheus'
      Yabeda::Kafka.registry = ::Yabeda::Kafka::Proxy.new(self)
      ::Kafka::Prometheus.start(Yabeda::Kafka.registry)
    end
  end
end
