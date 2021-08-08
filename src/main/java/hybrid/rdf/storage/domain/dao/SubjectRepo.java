package hybrid.rdf.storage.domain.dao;

import lombok.Data;
import org.springframework.stereotype.Repository;

@Repository
public interface SubjectRepo {
    void save(String s);

    Integer findIdByName(String name);
}
