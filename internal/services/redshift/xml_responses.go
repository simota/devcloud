package redshift

import "encoding/xml"

type describeClustersResponse struct {
	XMLName   xml.Name     `xml:"DescribeClustersResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"ResponseMetadata>RequestId"`
	Clusters  []clusterXML `xml:"DescribeClustersResult>Clusters>member"`
}

type getClusterCredentialsResponse struct {
	XMLName    xml.Name `xml:"GetClusterCredentialsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	DbUser     string   `xml:"GetClusterCredentialsResult>DbUser"`
	DbPassword string   `xml:"GetClusterCredentialsResult>DbPassword"`
	Expiration string   `xml:"GetClusterCredentialsResult>Expiration"`
	RequestID  string   `xml:"ResponseMetadata>RequestId"`
}

type clusterActionResponse struct {
	XMLName    xml.Name
	Xmlns      string
	RequestID  string
	ResultName xml.Name
	Cluster    clusterXML
}

type clusterSnapshotActionResponse struct {
	XMLName    xml.Name
	Xmlns      string
	RequestID  string
	ResultName xml.Name
	Snapshot   clusterSnapshotXML
}

type emptyQueryResponse struct {
	XMLName   xml.Name
	Xmlns     string `xml:"xmlns,attr"`
	RequestID string `xml:"ResponseMetadata>RequestId"`
}

func (r clusterActionResponse) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = r.XMLName
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "xmlns"}, Value: r.Xmlns})
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	result := struct {
		Cluster clusterXML `xml:"Cluster"`
	}{Cluster: r.Cluster}
	if err := e.EncodeElement(result, xml.StartElement{Name: r.ResultName}); err != nil {
		return err
	}
	metadata := struct {
		RequestID string `xml:"RequestId"`
	}{RequestID: r.RequestID}
	if err := e.EncodeElement(metadata, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

func (r clusterSnapshotActionResponse) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = r.XMLName
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "xmlns"}, Value: r.Xmlns})
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	result := struct {
		Snapshot clusterSnapshotXML `xml:"Snapshot"`
	}{Snapshot: r.Snapshot}
	if err := e.EncodeElement(result, xml.StartElement{Name: r.ResultName}); err != nil {
		return err
	}
	metadata := struct {
		RequestID string `xml:"RequestId"`
	}{RequestID: r.RequestID}
	if err := e.EncodeElement(metadata, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

type clusterXML struct {
	ClusterIdentifier string      `xml:"ClusterIdentifier"`
	ClusterStatus     string      `xml:"ClusterStatus"`
	DBName            string      `xml:"DBName"`
	Endpoint          endpointXML `xml:"Endpoint"`
	NodeType          string      `xml:"NodeType"`
	NumberOfNodes     int         `xml:"NumberOfNodes"`
	MasterUsername    string      `xml:"MasterUsername"`
}

type describeClusterSnapshotsResponse struct {
	XMLName   xml.Name             `xml:"DescribeClusterSnapshotsResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	Snapshots []clusterSnapshotXML `xml:"DescribeClusterSnapshotsResult>Snapshots>member"`
	RequestID string               `xml:"ResponseMetadata>RequestId"`
}

type clusterSnapshotXML struct {
	SnapshotIdentifier string `xml:"SnapshotIdentifier"`
	ClusterIdentifier  string `xml:"ClusterIdentifier"`
	SnapshotCreateTime string `xml:"SnapshotCreateTime"`
	Status             string `xml:"Status"`
	Port               int    `xml:"Port"`
	AvailabilityZone   string `xml:"AvailabilityZone"`
	ClusterCreateTime  string `xml:"ClusterCreateTime"`
	MasterUsername     string `xml:"MasterUsername"`
	ClusterVersion     string `xml:"ClusterVersion"`
	EngineFullVersion  string `xml:"EngineFullVersion"`
	NodeType           string `xml:"NodeType"`
	NumberOfNodes      int    `xml:"NumberOfNodes"`
	DBName             string `xml:"DBName"`
	Encrypted          bool   `xml:"Encrypted"`
}

func clusterSnapshotXMLFromMetadata(snapshot ClusterSnapshotMetadata) clusterSnapshotXML {
	return clusterSnapshotXML{
		SnapshotIdentifier: snapshot.SnapshotIdentifier,
		ClusterIdentifier:  snapshot.ClusterIdentifier,
		SnapshotCreateTime: snapshot.SnapshotCreateTime,
		Status:             snapshot.Status,
		Port:               snapshot.Port,
		AvailabilityZone:   snapshot.AvailabilityZone,
		ClusterCreateTime:  snapshot.ClusterCreateTime,
		MasterUsername:     snapshot.MasterUsername,
		ClusterVersion:     snapshot.ClusterVersion,
		EngineFullVersion:  snapshot.EngineFullVersion,
		NodeType:           snapshot.NodeType,
		NumberOfNodes:      snapshot.NumberOfNodes,
		DBName:             snapshot.DBName,
		Encrypted:          snapshot.Encrypted,
	}
}

type endpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type describeTagsResponse struct {
	XMLName         xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	TaggedResources []taggedResourceXML `xml:"DescribeTagsResult>TaggedResources>member"`
	RequestID       string              `xml:"ResponseMetadata>RequestId"`
}

type taggedResourceXML struct {
	ResourceName string `xml:"ResourceName"`
	ResourceType string `xml:"ResourceType"`
	Tag          Tag    `xml:"Tag"`
}

const defaultParameterGroupName = "default.redshift-1.0"

type describeClusterParameterGroupsResponse struct {
	XMLName         xml.Name            `xml:"DescribeClusterParameterGroupsResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	ParameterGroups []parameterGroupXML `xml:"DescribeClusterParameterGroupsResult>ParameterGroups>member"`
	RequestID       string              `xml:"ResponseMetadata>RequestId"`
}

type parameterGroupXML struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterGroupFamily string `xml:"ParameterGroupFamily"`
	Description          string `xml:"Description"`
}

type describeClusterParametersResponse struct {
	XMLName    xml.Name       `xml:"DescribeClusterParametersResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	Parameters []parameterXML `xml:"DescribeClusterParametersResult>Parameters>member"`
	RequestID  string         `xml:"ResponseMetadata>RequestId"`
}

type parameterXML struct {
	ParameterName        string `xml:"ParameterName"`
	ParameterValue       string `xml:"ParameterValue"`
	Description          string `xml:"Description"`
	Source               string `xml:"Source"`
	DataType             string `xml:"DataType"`
	AllowedValues        string `xml:"AllowedValues,omitempty"`
	ApplyType            string `xml:"ApplyType"`
	IsModifiable         bool   `xml:"IsModifiable"`
	MinimumEngineVersion string `xml:"MinimumEngineVersion"`
}

func defaultParameterGroupXML() parameterGroupXML {
	return parameterGroupXML{
		ParameterGroupName:   defaultParameterGroupName,
		ParameterGroupFamily: "redshift-1.0",
		Description:          "Default devcloud Redshift-compatible parameter group",
	}
}

func defaultClusterParameters() []parameterXML {
	return []parameterXML{
		{
			ParameterName:        "datestyle",
			ParameterValue:       "ISO, MDY",
			Description:          "Sets the display format for date and time values.",
			Source:               "engine-default",
			DataType:             "string",
			ApplyType:            "static",
			IsModifiable:         false,
			MinimumEngineVersion: "1.0",
		},
		{
			ParameterName:        "enable_user_activity_logging",
			ParameterValue:       "false",
			Description:          "Controls user activity logging metadata for the local Redshift-compatible server.",
			Source:               "engine-default",
			DataType:             "boolean",
			AllowedValues:        "true,false",
			ApplyType:            "dynamic",
			IsModifiable:         false,
			MinimumEngineVersion: "1.0",
		},
		{
			ParameterName:        "max_query_execution_time",
			ParameterValue:       "0",
			Description:          "Maximum query execution time in seconds. Zero means unlimited in devcloud.",
			Source:               "engine-default",
			DataType:             "integer",
			AllowedValues:        "0-86400",
			ApplyType:            "dynamic",
			IsModifiable:         false,
			MinimumEngineVersion: "1.0",
		},
	}
}
