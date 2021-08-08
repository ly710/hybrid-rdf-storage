package hybrid.rdf.storage.domain.dao;

import org.springframework.stereotype.Repository;

@Repository
public interface ObjectRepo {
    void save(String o);

    Integer findIdByName(String name);
}
