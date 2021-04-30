package hybrid.rdf.storage.executor;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;

public class HybridGraphImpl extends GraphBase
{
    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern)
    {
        return null;
    }

    @Override
    public void performAdd(Triple t)
    {
        super.performAdd(t);
    }

    @Override
    public void performDelete(Triple t)
    {
        super.performDelete(t);
    }

    @Override
    protected int graphBaseSize()
    {
        return super.graphBaseSize();
    }
}
