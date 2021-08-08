package hybrid.rdf.storage.domain.model;

import lombok.Data;

@Data
public class LongTripleTuple
{
    private String s;

    private String p;

    private Long o;

    private Long rownum;
}
