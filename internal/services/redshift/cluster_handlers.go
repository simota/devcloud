package redshift

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"
)

func (s *Server) handleDescribeClusters(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	clusters := s.clusterXMLsLocked()
	s.mu.Unlock()
	response := describeClustersResponse{
		XMLName:   xml.Name{Local: "DescribeClustersResponse"},
		Xmlns:     "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID: "devcloud-redshift",
		Clusters:  clusters,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetClusterCredentials(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	identifier := defaultString(r.Form.Get("ClusterIdentifier"), defaultClusterIdentifier(s.config))
	s.mu.Lock()
	_, exists := s.clusters[identifier]
	s.mu.Unlock()
	if !exists {
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	durationSeconds := parseCredentialDurationSeconds(r.Form.Get("DurationSeconds"))
	response := getClusterCredentialsResponse{
		XMLName:    xml.Name{Local: "GetClusterCredentialsResponse"},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		DbUser:     defaultString(r.Form.Get("DbUser"), defaultString(s.config.User, "dev")),
		DbPassword: defaultString(s.config.Password, "dev"),
		Expiration: time.Now().UTC().Add(time.Duration(durationSeconds) * time.Second).Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	cluster := s.clusterSnapshotFromForm(r)
	s.mu.Lock()
	if _, exists := s.clusters[cluster.ClusterIdentifier]; exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusBadRequest, "ClusterAlreadyExists", "cluster already exists")
		return
	}
	s.clusters[cluster.ClusterIdentifier] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift cluster metadata failed")
		return
	}
	s.mu.Unlock()
	s.writeClusterActionXML(w, "CreateClusterResponse", "CreateClusterResult", cluster)
}

func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	identifier := defaultString(r.Form.Get("ClusterIdentifier"), defaultClusterIdentifier(s.config))
	s.mu.Lock()
	cluster, exists := s.clusters[identifier]
	if exists {
		delete(s.clusters, identifier)
	}
	if exists {
		if err := s.persistLocked(); err != nil {
			s.mu.Unlock()
			writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift cluster metadata failed")
			return
		}
	}
	s.mu.Unlock()
	if !exists {
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	s.writeClusterActionXML(w, "DeleteClusterResponse", "DeleteClusterResult", cluster)
}

func (s *Server) handleDescribeClusterSnapshots(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	clusterIdentifier := strings.TrimSpace(r.Form.Get("ClusterIdentifier"))
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	s.mu.Lock()
	snapshots := s.clusterSnapshotXMLsLocked(clusterIdentifier, snapshotIdentifier)
	s.mu.Unlock()
	response := describeClusterSnapshotsResponse{
		XMLName:   xml.Name{Local: "DescribeClusterSnapshotsResponse"},
		Xmlns:     "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID: "devcloud-redshift",
		Snapshots: snapshots,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	if snapshotIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "SnapshotIdentifier is required")
		return
	}
	clusterIdentifier := defaultString(strings.TrimSpace(r.Form.Get("ClusterIdentifier")), defaultClusterIdentifier(s.config))
	s.mu.Lock()
	cluster, exists := s.clusters[clusterIdentifier]
	if !exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	if _, exists := s.snapshots[snapshotIdentifier]; exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusBadRequest, "ClusterSnapshotAlreadyExists", "cluster snapshot already exists")
		return
	}
	snapshot := clusterSnapshotMetadataFromCluster(snapshotIdentifier, cluster)
	s.snapshots[snapshotIdentifier] = snapshot
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift snapshot metadata failed")
		return
	}
	s.mu.Unlock()
	s.writeClusterSnapshotActionXML(w, "CreateClusterSnapshotResponse", "CreateClusterSnapshotResult", snapshot)
}

func (s *Server) handleDeleteClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	if snapshotIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "SnapshotIdentifier is required")
		return
	}
	s.mu.Lock()
	snapshot, exists := s.snapshots[snapshotIdentifier]
	if exists {
		delete(s.snapshots, snapshotIdentifier)
	}
	if exists {
		if err := s.persistLocked(); err != nil {
			s.mu.Unlock()
			writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift snapshot metadata failed")
			return
		}
	}
	s.mu.Unlock()
	if !exists {
		writeJSONError(w, http.StatusNotFound, "ClusterSnapshotNotFound", "cluster snapshot does not exist")
		return
	}
	s.writeClusterSnapshotActionXML(w, "DeleteClusterSnapshotResponse", "DeleteClusterSnapshotResult", snapshot)
}

func (s *Server) handleRestoreFromClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	if snapshotIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "SnapshotIdentifier is required")
		return
	}
	clusterIdentifier := strings.TrimSpace(r.Form.Get("ClusterIdentifier"))
	if clusterIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "ClusterIdentifier is required")
		return
	}

	s.mu.Lock()
	snapshot, exists := s.snapshots[snapshotIdentifier]
	if !exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterSnapshotNotFound", "cluster snapshot does not exist")
		return
	}
	if _, exists := s.clusters[clusterIdentifier]; exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusBadRequest, "ClusterAlreadyExists", "cluster already exists")
		return
	}
	cluster := clusterSnapshotFromSnapshotMetadata(clusterIdentifier, snapshot, s.config)
	s.clusters[clusterIdentifier] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift restored cluster metadata failed")
		return
	}
	s.mu.Unlock()

	s.writeClusterActionXML(w, "RestoreFromClusterSnapshotResponse", "RestoreFromClusterSnapshotResult", cluster)
}

func (s *Server) handleDescribeTags(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	resourceName := r.Form.Get("ResourceName")
	s.mu.Lock()
	taggedResources := s.taggedResourcesLocked(resourceName)
	s.mu.Unlock()
	response := describeTagsResponse{
		XMLName:         xml.Name{Local: "DescribeTagsResponse"},
		Xmlns:           "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:       "devcloud-redshift",
		TaggedResources: taggedResources,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleDescribeClusterParameterGroups(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	name := strings.TrimSpace(r.Form.Get("ParameterGroupName"))
	groups := []parameterGroupXML{defaultParameterGroupXML()}
	if name != "" && name != groups[0].ParameterGroupName {
		groups = nil
	}
	response := describeClusterParameterGroupsResponse{
		XMLName:         xml.Name{Local: "DescribeClusterParameterGroupsResponse"},
		Xmlns:           "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:       "devcloud-redshift",
		ParameterGroups: groups,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleDescribeClusterParameters(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	name := defaultString(strings.TrimSpace(r.Form.Get("ParameterGroupName")), defaultParameterGroupName)
	if name != defaultParameterGroupName {
		writeJSONError(w, http.StatusNotFound, "ClusterParameterGroupNotFound", "cluster parameter group does not exist")
		return
	}
	response := describeClusterParametersResponse{
		XMLName:    xml.Name{Local: "DescribeClusterParametersResponse"},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		Parameters: defaultClusterParameters(),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateTags(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	resourceName := r.Form.Get("ResourceName")
	if resourceName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "ResourceName is required")
		return
	}
	tags := parseTagMembers(r.Form)
	if len(tags) == 0 {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Tags are required")
		return
	}
	s.mu.Lock()
	cluster, id, ok := s.clusterByResourceNameLocked(resourceName)
	if !ok {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	cluster.Tags = mergeTags(cluster.Tags, tags)
	s.clusters[id] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift tag metadata failed")
		return
	}
	s.mu.Unlock()
	writeEmptyQueryXML(w, "CreateTagsResponse")
}

func (s *Server) handleDeleteTags(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	resourceName := r.Form.Get("ResourceName")
	if resourceName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "ResourceName is required")
		return
	}
	keys := parseTagKeyMembers(r.Form)
	if len(keys) == 0 {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "TagKeys are required")
		return
	}
	s.mu.Lock()
	cluster, id, ok := s.clusterByResourceNameLocked(resourceName)
	if !ok {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	cluster.Tags = deleteTags(cluster.Tags, keys)
	s.clusters[id] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift tag metadata failed")
		return
	}
	s.mu.Unlock()
	writeEmptyQueryXML(w, "DeleteTagsResponse")
}
