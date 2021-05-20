# frozen_string_literal: true

RSpec.describe Yabeda::Kafka do
  before(:all) { Yabeda.configure! }

  it 'has a version number' do
    expect(Yabeda::Kafka::VERSION).not_to be nil
  end

  it 'does something useful' do
    expect(Yabeda.kafka.api_calls).to be_kind_of(::Yabeda::Counter)
  end

  describe 'metrics hooks' do
    let(:exception) { false }
    let(:registry) { Yabeda.kafka }

    before do
      # clear yabeda metrics
      Yabeda::Kafka.registry.metrics.each { |m| registry.send(m).values.clear }

      instrumenter = if exception
                       Kafka::Instrumenter.new(client_id: 'test', exception: exception)
                     else
                       Kafka::Instrumenter.new(client_id: 'test')
                     end
      instrumenter.instrument(hook, payload)
    end

    context 'when requesting a connection' do
      let(:key) { { client: 'test', api: 'foo', broker: 'somehost' } }
      let(:payload) { { broker_host: 'somehost', api: 'foo', request_size: 101, response_size: 4000 } }
      let(:hook) { 'request.connection' }

      it 'emits metrics to the api_calls' do
        expect(registry.api_calls.values[key]).to eq 1
      end

      it 'emits metrics to api_latency' do
        expect(registry.api_latency.values[key]).to be_kind_of(Float)
      end

      it 'emits metrics to api_request_size' do
        expect(registry.api_request_size.values[key]).not_to be_nil
      end

      it 'emits metrics to api_response_size' do
        expect(registry.api_response_size.values[key]).not_to be_nil
      end

      context 'with expection' do
        let(:exception) { true }

        it 'emits metrics to api_errors' do
          expect(registry.api_errors.values[key]).to eq 1
        end
      end
    end

    context 'when a consumer is processing a message' do
      let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
      let(:payload) do
        {
          group_id: 'group1',
          topic: 'AAA',
          partition: 4,
          offset: 1,
          offset_lag: 500,
          create_time: Time.now - 5
        }
      end
      let(:hook) { 'process_message.consumer' }

      it 'emits metrics to consumer_offset_lag' do
        expect(registry.consumer_offset_lag.values[key]).to eq 500
      end

      it 'emits metrics to consumer_process_messages' do
        expect(registry.consumer_process_messages.values[key]).to eq 1
      end

      it 'emits metrics to consumer_process_message_latency' do
        expect(registry.consumer_process_message_latency.values[key]).not_to be_nil
      end

      it 'emits metrics to consumer_time_lag' do
        expect(registry.consumer_time_lag.values[key]).to be > 0
      end

      context 'with expection' do
        let(:exception) { true }

        it 'emits metrics to consumer_process_message_errors' do
          expect(registry.consumer_process_message_errors.values[key]).to eq 1
        end
      end
    end

    context 'when a consumer is processing a batch' do
      let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
      let(:payload) do
        {
          group_id: 'group1',
          topic: 'AAA',
          partition: 4,
          last_offset: 100,
          last_create_time: Time.now,
          message_count: 7
        }
      end
      let(:hook) { 'process_batch.consumer' }

      it 'emits metrics consumer_process_messages' do
        expect(registry.consumer_process_messages.values[key]).to eq 7
      end

      context 'with expection' do
        let(:exception) { true }

        it 'emits metrics to consumer_process_batch_errors' do
          expect(registry.consumer_process_batch_errors.values[key]).to eq 1
        end
      end
    end

    context 'when a consumer is fetching a batch' do
      let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
      let(:payload) do
        {
          group_id: 'group1',
          topic: 'AAA',
          partition: 4,
          offset_lag: 7,
          message_count: 123
        }
      end
      let(:hook) { 'fetch_batch.consumer' }

      it 'emits metrics consumer_offset_lag' do
        expect(registry.consumer_offset_lag.values[key]).to eq 7
      end

      it 'emits metrics consumer_batch_size' do
        expect(registry.consumer_batch_size.values[key]).to be > 0
      end
    end

    context 'when a consumer is joining a group' do
      let(:key) { { client: 'test', group_id: 'group1' } }
      let(:payload) { { group_id: 'group1' } }
      let(:hook) { 'join_group.consumer' }

      it 'emits metrics consumer_join_group' do
        expect(registry.consumer_join_group.values[key]).to be > 0
      end

      context 'with expection' do
        let(:exception) { true }

        it 'emits metrics to consumer_join_group_errors' do
          expect(registry.consumer_join_group_errors.values[key]).to eq 1
        end
      end
    end

    context 'when a consumer is syncing a group' do
      let(:key) { { client: 'test', group_id: 'group1' } }
      let(:payload) { { group_id: 'group1' } }
      let(:hook) { 'sync_group.consumer' }

      it 'emits metrics consumer_sync_group' do
        expect(registry.consumer_sync_group.values[key]).to be > 0
      end

      context 'with expection' do
        let(:exception) { true }

        it 'emits metrics to consumer_sync_group_errors' do
          expect(registry.consumer_sync_group_errors.values[key]).to eq 1
        end
      end
    end

    context 'when a consumer is leaving a group' do
      let(:key) { { client: 'test', group_id: 'group1' } }
      let(:payload) { { group_id: 'group1' } }
      let(:hook) { 'leave_group.consumer' }

      it 'emits metrics consumer_leave_group' do
        expect(registry.consumer_leave_group.values[key]).to be > 0
      end

      context 'with expection' do
        let(:exception) { true }

        it 'emits metrics to consumer_leave_group_errors' do
          expect(registry.consumer_leave_group_errors.values[key]).to eq 1
        end
      end
    end

    context 'when a consumer pauses status' do
      let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
      let(:payload) { { group_id: 'group1', topic: 'AAA', partition: 4, duration: 111 } }
      let(:hook) { 'pause_status.consumer' }

      it 'emits metrics to consumer_pause_duration' do
        expect(registry.consumer_pause_duration.values[key]).to eq 111
      end
    end

    context 'when a producer produces a message' do
      let(:key) { { client: 'test', topic: 'AAA' } }
      let(:payload) do
        {
          group_id: 'group1',
          topic: 'AAA',
          partition: 4,
          buffer_size: 1000,
          max_buffer_size: 10_000,
          message_size: 123
        }
      end
      let(:hook) { 'produce_message.producer' }

      it 'emits metrics producer_produced_messages' do
        expect(registry.producer_produced_messages.values[key]).to eq 1
      end

      it 'emits metric producer_message_size' do
        expect(registry.producer_message_size.values[key]).to be > 0
      end

      it 'emits metric buffer_fill_ratio' do
        expect(registry.producer_buffer_fill_ratio.values[{ client: 'test' }]).to be > 0
      end
    end

    context 'when a producer gets topic error' do
      let(:key) { { client: 'test', topic: 'AAA' } }
      let(:payload) { { group_id: 'group1', topic: 'AAA' } }
      let(:hook) { 'topic_error.producer' }

      it 'emits metrics ack_error' do
        expect(registry.producer_ack_errors.values[key]).to eq 1
      end
    end

    context 'when a producer gets buffer overflow' do
      let(:key) { { client: 'test', topic: 'AAA' } }
      let(:payload) { { topic: 'AAA' } }
      let(:hook) { 'buffer_overflow.producer' }

      it 'emits metrics producer_produce_errors' do
        expect(registry.producer_produce_errors.values[key]).to eq 1
      end
    end

    context 'when a producer deliver_messages' do
      let(:key) { { client: 'test' } }
      let(:payload) { { delivered_message_count: 123, attempts: 2 } }
      let(:hook) { 'deliver_messages.producer' }

      it 'emits metrics producer_deliver_messages' do
        expect(registry.producer_deliver_messages.values[key]).to eq 123
      end

      it 'emits metrics producer_deliver_attempts' do
        expect(registry.producer_deliver_attempts.values[key]).to be > 0
      end
    end

    context 'when a asynch producer enqueues a message' do
      let(:key) { { client: 'test', topic: 'AAA' } }
      let(:payload) { { group_id: 'group1', topic: 'AAA' } }
      let(:hook) { 'topic_error.async_producer' }

      it 'emits metrics async_producer_queue_size' do
        expect(registry.async_producer_queue_size.values).not_to be_nil
      end

      it 'emits metrics async_producer_queue fill_ratio' do
        expect(registry.async_producer_queue_fill_ratio.values).not_to be_nil
      end
    end

    context 'when a asynch producer gets buffer overflow' do
      let(:key) { { client: 'test', topic: 'AAA' } }
      let(:payload) { { topic: 'AAA' } }
      let(:hook) { 'buffer_overflow.async_producer' }

      it 'emits metrics async_producer_produce_errors' do
        expect(registry.async_producer_produce_errors.values[key]).to eq 1
      end
    end

    context 'when a asynch producer gets dropped messages' do
      let(:key) { { client: 'test' } }
      let(:payload) { { message_count: 4 } }
      let(:hook) { 'drop_messages.async_producer' }

      it 'emits metrics async_producer_dropped_messages' do
        expect(registry.async_producer_dropped_messages.values[key]).to eq 4
      end
    end
  end
end
