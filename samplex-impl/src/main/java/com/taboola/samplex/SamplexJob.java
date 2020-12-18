package samplex;


import com.taboola.schemafilter.SchemaFilter;

public interface SamplexJob {

    SamplexFilter getRecordFilter();

    String getDestinationFolder();

    default SchemaFilter getSchemaFilter(){
        return null;
    }
}
