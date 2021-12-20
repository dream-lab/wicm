package in.dreamlab.wicm.graph.mutations.resolver;

import in.dreamlab.wicm.conf.WICMConstants;
import in.dreamlab.wicm.edge.ByteArrayEdgesClearable;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public abstract class WICMVertexResolver<I extends WritableComparable, V extends Writable, E extends Writable>
        extends DefaultVertexResolver<I, V, E>
        implements WICMConstants {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(WICMVertexResolver.class);

    @Override
    public Vertex<I, V, E> resolve(
            I vertexId,
            Vertex<I, V, E> vertex,
            VertexChanges<I, V, E> vertexChanges,
            boolean hasMessages) {
        // 2. If vertex removal desired, remove the vertex.
        vertex = removeVertexIfDesired(vertex, vertexChanges);

        // 3. If creation of vertex desired, pick first vertex
        // 4. If vertex doesn't exist, but got messages or added edges, create
        vertex = addVertexIfDesired(vertexId, vertex, vertexChanges, hasMessages);

        // 5. If edge addition, add the edges
        addEdges(vertex, vertexChanges);

        return vertex;
    }

    /**
     * Remove edges as specifed in changes given.
     *
     * @param vertex Vertex to remove edges from
     * @param vertexChanges contains list of edges to remove.
     */
    protected void removeEdges(Vertex<I, V, E> vertex,
                               VertexChanges<I, V, E> vertexChanges) {
        if (vertex == null) {
            return;
        }
        if (hasEdgeRemovals(vertexChanges)) {
            for (I removedDestVertex : vertexChanges.getRemovedEdgeList()) {
                vertex.removeEdges(removedDestVertex);
            }
        }
    }

    /**
     * Remove the vertex itself if the changes desire it. The actual removal is
     * notified by returning null. That is, this method does not do the actual
     * removal but rather returns null if it should be done.
     *
     * @param vertex Vertex to remove.
     * @param vertexChanges specifies if we should remove vertex
     * @return null if vertex should be removed, otherwise the vertex itself.
     */
    protected Vertex<I, V, E> removeVertexIfDesired(
            Vertex<I, V, E> vertex,
            VertexChanges<I, V, E> vertexChanges) {
        if (hasVertexRemovals(vertexChanges)) {
            try {
                Text vertexStates = new Text(vertexToString(vertex));
                resolverOutput.write(vertexStates);
            } catch (Exception e) {
                LOG.info("Caught exception "+e);
            }
            vertex = null;
        }
        return vertex;
    }

    protected abstract String vertexToString(Vertex<I,V,E> v);

    /**
     * Add the Vertex if desired. Returns the vertex itself, or null if no vertex
     * added.
     *
     * @param vertexId ID of vertex
     * @param vertex Vertex, if not null just returns it as vertex already exists
     * @param vertexChanges specifies if we should add the vertex
     * @param hasMessages true if this vertex received any messages
     * @return Vertex created or passed in, or null if no vertex should be added
     */
    protected Vertex<I, V, E> addVertexIfDesired(
            I vertexId,
            Vertex<I, V, E> vertex,
            VertexChanges<I, V, E> vertexChanges,
            boolean hasMessages) {
        if (vertex == null && hasVertexAdditions(vertexChanges)) {
            vertex = vertexChanges.getAddedVertexList().get(0);
            initialiseState(vertex);
        } else if (hasVertexAdditions(vertexChanges)) {
            customAction(vertex, vertexChanges.getAddedVertexList().get(0));
        }
        return vertex;
    }

    protected abstract void initialiseState(Vertex<I,V,E> v);

    protected abstract void customAction(Vertex<I,V,E> originalVertex, Vertex<I,V,E> newVertex);

    /**
     * Add edges to the Vertex.
     *
     * @param vertex Vertex to add edges to
     * @param vertexChanges contains edges to add
     */
    protected void addEdges(Vertex<I, V, E> vertex,
                            VertexChanges<I, V, E> vertexChanges) {
        if (vertex == null) {
            return;
        }

        if (hasEdgeAdditions(vertexChanges)) {
            if (vertex.getEdges() instanceof ByteArrayEdgesClearable) {
                ((ByteArrayEdgesClearable<I, E>) vertex.getEdges()).removeAllEdges();
                ((ByteArrayEdgesClearable<I, E>) vertex.getEdges()).add(vertexChanges.getAddedEdgeList());
                ((ByteArrayEdgesClearable<I, E>) vertex.getEdges()).trim();
            } else {
                vertex.setEdges(vertexChanges.getAddedEdgeList());
            }
        }
    }

    /**
     * Check if changes contain vertex removal requests
     *
     * @param changes VertexChanges to check
     * @return true if changes contains vertex removal requests
     */
    protected  boolean hasVertexRemovals(VertexChanges<I, V, E> changes) {
        return changes != null && changes.getRemovedVertexCount() > 0;
    }

    /**
     * Check if changes contain vertex addition requests
     *
     * @param changes VertexChanges to check
     * @return true if changes contains vertex addition requests
     */
    protected boolean hasVertexAdditions(VertexChanges<I, V, E> changes) {
        return changes != null && !changes.getAddedVertexList().isEmpty();
    }

    /**
     * Check if changes contain edge addition requests
     *
     * @param changes VertexChanges to check
     * @return true if changes contains edge addition requests
     */
    protected boolean hasEdgeAdditions(VertexChanges<I, V, E> changes) {
        return changes != null && !changes.getAddedEdgeList().isEmpty();
    }

    /**
     * Check if changes contain edge removal requests
     *
     * @param changes VertexChanges to check
     * @return true if changes contains edge removal requests
     */
    protected boolean hasEdgeRemovals(VertexChanges<I, V, E> changes) {
        return changes != null && !changes.getRemovedEdgeList().isEmpty();
    }
}
