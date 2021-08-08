package hybrid.rdf.storage.domain.dao;

import org.springframework.stereotype.Repository;

@Repository
public interface PredictRepo
{
    void save(String s);
}
