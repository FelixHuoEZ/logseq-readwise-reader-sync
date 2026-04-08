import type { PageRenderContext, RenderedPage } from './types'
import { buildRenderHashInput } from './build-render-hash-input'
import { buildSemanticPage } from './build-semantic-page'
import { computeRenderHash } from './compute-render-hash'
import { emitOrgPage } from './emit-org-page'

export const renderPage = (
  context: PageRenderContext,
  computeUuid: (locationUrl: string) => string,
): RenderedPage => {
  const semanticPage = buildSemanticPage(context, computeUuid)
  const renderHashInput = buildRenderHashInput(semanticPage)
  const renderHash = computeRenderHash(renderHashInput)
  const emitResult = emitOrgPage(semanticPage)

  return {
    format: context.runtime.format,
    userBookId: context.book.userBookId,
    pageTitle: semanticPage.pageTitle,
    semanticPage,
    emitResult,
    renderHashInput,
    renderHash,
  }
}
