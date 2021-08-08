package hybrid.rdf.storage.domain.dao;

import org.springframework.stereotype.Repository;

@Repository
public interface SpoDaoSRepo
{
    void save(String s, String p, String o);
}
