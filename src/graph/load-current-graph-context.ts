export interface CurrentGraphContextV1 {
  graphId: string
  graphName: string
}

const fallbackGraphId = () => window.location.pathname || 'unknown-graph'

const fallbackGraphName = () =>
  document.title.trim() || window.location.pathname || 'Current Graph'

export const loadCurrentGraphContextV1 =
  async (): Promise<CurrentGraphContextV1> => {
    const graph = await logseq.App.getCurrentGraph()

    return {
      graphId: graph?.path ?? fallbackGraphId(),
      graphName: graph?.name ?? fallbackGraphName(),
    }
  }
