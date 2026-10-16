/** Exercise Messages request conversion with recorded responses from another protocol. */
import type { Context } from '@deepseek-ai/cordis'
import { resolveAdapterOptions } from '../../src/config.ts'
import { serialize } from '../../src/serialize.ts'
import { translate } from '../../src/translate.ts'
import type { StreamChunk } from '@deepseek-ai/dsh-llm'

export const name = 'messages-history-snapshot'
export const inject = ['llm']

export function apply(ctx: Context): void {
  const connection = resolveAdapterOptions({})
  ctx.on('llm/stream', (options, next) => {
    const request = serialize(options, connection, options.messages, new Map(), () => undefined)
    if (request.messages.some(message => message.content.some(block => block.type === 'text' && block.text.length === 0))) {
      throw new Error('Messages history contains an empty text block')
    }
    return translatedToolHistory(next())
  })
}

/** Verify that recorded tool arguments survive the Messages stream translator before execution. */
async function* translatedToolHistory(stream: AsyncIterable<StreamChunk>): AsyncGenerator<StreamChunk> {
  for await (const chunk of stream) {
    if (chunk.type === 'block-end' && chunk.block.type === 'tool-call') {
      const block = chunk.block
      const events = [
        { type: 'message_start', message: { usage: {} } },
        { type: 'content_block_start', index: 0, content_block: { type: 'tool_use', id: block.id, name: block.name, input: {} } },
        { type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: block.arguments } },
        { type: 'content_block_stop', index: 0 },
        { type: 'message_delta', delta: { stop_reason: 'tool_use' } },
        { type: 'message_stop' },
      ]
      for await (const translated of translate((async function* () { yield* events })(), 'deepseek-v4-flash')) {
        if (translated.type === 'block-end' && translated.block.type === 'tool-call'
          && translated.block.arguments !== block.arguments) throw new Error('Messages changed recorded tool arguments')
      }
    }
    yield chunk
  }
}
