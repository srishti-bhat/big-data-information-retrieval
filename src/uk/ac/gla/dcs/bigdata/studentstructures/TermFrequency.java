package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class TermFrequency implements Serializable {
    private static final long serialVersionUID = 1565312120561246290L;

    String term;
    Long totalCount;

    public TermFrequency(String term, Long totalCount) {
        this.term = term;
        this.totalCount = totalCount;
    }

    public TermFrequency() {
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }
}
