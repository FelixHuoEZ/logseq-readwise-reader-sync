import type { NormalizedBookExport } from '../normalizer'
import type { PageRenderContext, RenderRuntimeContext } from './types'

export const buildPageRenderContext = (
  book: NormalizedBookExport,
  runtime: RenderRuntimeContext,
): PageRenderContext => ({
  book,
  runtime,
})
