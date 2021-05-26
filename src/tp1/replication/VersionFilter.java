package tp1.replication;

import java.io.IOException;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import tp1.api.service.rest.ReplicationRestSpreadsheets;

@Provider
public class VersionFilter implements ContainerResponseFilter {
    ReplicationManager replicationM;

    public VersionFilter(ReplicationManager replicationM) {
        this.replicationM = replicationM;
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) 											//COMO UTILIZAR??
                throws IOException {
    	response.getHeaders().add(ReplicationRestSpreadsheets.HEADER_VERSION, replicationM.getGlobalSequenceNumber());
    }

}
