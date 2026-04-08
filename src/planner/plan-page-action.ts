import type { GraphStateV1 } from '../state'
import type { PageSyncActionV1, PlannerInputItemV1 } from './types'

const buildActionId = (
  userBookId: number,
  actionType: PageSyncActionV1['type'],
) => `${actionType}:${userBookId}`

const createSkipAction = (
  item: PlannerInputItemV1,
  skipReason: Extract<PageSyncActionV1, { type: 'skip_page' }>['skipReason'],
  reasonCode: PageSyncActionV1['reasonCode'],
  reasonDetail: string,
): Extract<PageSyncActionV1, { type: 'skip_page' }> => ({
  type: 'skip_page',
  actionId: buildActionId(item.book.userBookId, 'skip_page'),
  userBookId: item.book.userBookId,
  remoteTitle: item.book.title,
  reasonCode,
  reasonDetail,
  blocksCheckpoint: skipReason === 'manual_pause',
  skipReason,
  pageUuid: item.existingPageIndexEntry?.pageUuid ?? null,
  renderedPage: item.renderedPage,
})

export const planPageActionV1 = (
  item: PlannerInputItemV1,
  _graphState: GraphStateV1,
): PageSyncActionV1 => {
  const { book, renderedPage, existingPageIndexEntry } = item

  if (item.acceptedPendingRelink) {
    return {
      type: 'relink_page',
      actionId: buildActionId(book.userBookId, 'relink_page'),
      userBookId: book.userBookId,
      remoteTitle: book.title,
      reasonCode: 'title_exact_match_pending_confirmation',
      reasonDetail: 'A previously accepted manual relink candidate exists.',
      blocksCheckpoint: false,
      targetPageUuid: item.acceptedPendingRelink.candidatePageUuid,
      candidatePageTitle: item.acceptedPendingRelink.candidatePageTitle,
      renderedPage,
    }
  }

  if (existingPageIndexEntry) {
    if (book.isDeleted) {
      if (item.mappedPageExists) {
        return {
          type: 'mark_page_deleted',
          actionId: buildActionId(book.userBookId, 'mark_page_deleted'),
          userBookId: book.userBookId,
          remoteTitle: book.title,
          reasonCode: 'remote_deleted_local_exists',
          reasonDetail:
            'Remote book is deleted and the mapped local page still exists.',
          blocksCheckpoint: false,
          pageUuid: existingPageIndexEntry.pageUuid,
          archiveNamespace: 'Readwise Archived',
        }
      }

      return createSkipAction(
        item,
        'local_missing_but_deleted_remote',
        'remote_deleted_local_missing',
        'Remote book is deleted and the mapped local page is already missing.',
      )
    }

    if (item.mappedPageExists) {
      if (existingPageIndexEntry.lastAppliedRenderHash === renderedPage.renderHash) {
        return createSkipAction(
          item,
          'remote_unchanged',
          'mapped_page_exists_remote_unchanged',
          'The mapped page exists and the rendered hash is unchanged.',
        )
      }

      return {
        type: 'update_page',
        actionId: buildActionId(book.userBookId, 'update_page'),
        userBookId: book.userBookId,
        remoteTitle: book.title,
        reasonCode: 'mapped_page_exists_remote_changed',
        reasonDetail:
          'The mapped page exists and the rendered hash differs from the last applied hash.',
        blocksCheckpoint: false,
        pageUuid: existingPageIndexEntry.pageUuid,
        renderedPage,
        previousRenderHash: existingPageIndexEntry.lastAppliedRenderHash,
        nextRenderHash: renderedPage.renderHash,
      }
    }

    if (item.exactTitleCandidates.length > 0) {
      return createSkipAction(
        item,
        'awaiting_manual_relink',
        'title_exact_match_pending_confirmation',
        'The mapped page is missing but exact-title candidates exist and require manual confirmation.',
      )
    }

    return createSkipAction(
      item,
      'awaiting_manual_relink',
      'mapped_page_missing',
      'The mapped page is missing and no trusted automatic recovery path is available.',
    )
  }

  if (book.isDeleted) {
    return createSkipAction(
      item,
      'local_missing_but_deleted_remote',
      'remote_deleted_local_missing',
      'Remote book is deleted and there is no local mapping to archive.',
    )
  }

  if (item.exactTitleCandidates.length > 0) {
    return createSkipAction(
      item,
      'awaiting_manual_relink',
      'title_exact_match_pending_confirmation',
      'An exact-title local candidate exists and requires manual confirmation before any write.',
    )
  }

  return {
    type: 'create_page',
    actionId: buildActionId(book.userBookId, 'create_page'),
    userBookId: book.userBookId,
    remoteTitle: book.title,
    reasonCode: 'missing_mapping',
    reasonDetail:
      'No mapping or trusted relink candidate exists, so a new page should be created.',
    blocksCheckpoint: false,
    renderedPage,
  }
}
