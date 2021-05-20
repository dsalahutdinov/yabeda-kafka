# frozen_string_literal: true

module Yabeda
  module Kafka
    class Proxy
      attr_reader :group, :metrics

      def initialize(group)
        @group = group
        @metrics = []
      end

      def counter(name, docstring:, labels:)
        metrics.push name

        ::Yabeda::Kafka::Proxy::Counter.new(
          group.counter(name, comment: docstring, tags: labels)
        )
      end

      def histogram(name, docstring:, labels:, buckets: nil)
        metrics.push name

        ::Yabeda::Kafka::Proxy::Histogram.new(
          group.histogram(
            name,
            comment: docstring,
            tags: labels,
            buckets: buckets
          )
        )
      end

      def gauge(name, docstring:, labels:)
        metrics.push name
        ::Yabeda::Kafka::Proxy::Gauge.new(
          group.gauge(name, tags: labels, comment: docstring)
        )
      end

      class Counter
        attr_reader :yabeda_counter

        def initialize(yabeda_counter)
          @yabeda_counter = yabeda_counter
        end

        def increment(labels:, by: 1)
          yabeda_counter.increment(labels, by: by)
        end
      end

      class Histogram
        attr_reader :yabeda_histogram

        def initialize(yabeda_histogram)
          @yabeda_histogram = yabeda_histogram
        end

        def observe(value, labels:)
          yabeda_histogram.measure(labels, value)
        end
      end

      class Gauge
        attr_reader :yabeda_gauge

        def initialize(yabeda_gauge)
          @yabeda_gauge = yabeda_gauge
        end

        def set(value, labels:)
          yabeda_gauge.set(labels, value)
        end
      end
    end
  end
end
