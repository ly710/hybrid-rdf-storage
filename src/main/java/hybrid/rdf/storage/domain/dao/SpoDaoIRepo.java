package hybrid.rdf.storage.domain.dao;

import hybrid.rdf.storage.domain.model.LongTripleTuple;
import hybrid.rdf.storage.domain.model.NumberPropertyInfo;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

@Repository
public interface SpoDaoIRepo
{
    void save(String s, String p, Integer o);

    NumberPropertyInfo getNumberPropertyInfo(String predict);

    List<LongTripleTuple> getNumberPropertySampleList(Long mod, String predict);

    Set<Integer> getSidByORange(Long startRow, Long endRow, String predict);
}
