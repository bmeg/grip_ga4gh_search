package main

import (
  "fmt"
  "log"
  "net"
  "context"
  "io/ioutil"
  "gopkg.in/resty.v1"
  "encoding/json"
  "github.com/bmeg/grip/gripper"
  "google.golang.org/grpc"
  "github.com/spf13/cobra"
  "github.com/ghodss/yaml"
  "google.golang.org/protobuf/types/known/structpb"
)

type DataModelRef struct {
  Ref  string   `json:"$ref"`
}

type TableDescription struct {
  Name string  `json:"name"`
  DataModel DataModelRef `json:"data_model"`
}

type Pagination struct {
  NextPageURL  string `json:"next_page_url"`
}

type TablesResponse struct {
  Tables []TableDescription  `json:"tables"`
  Pagination  *Pagination     `json:"pagination"`
}

type TableInfo struct {
  Name string `json:"name"`
  Description string `json:"description"`
  DataModel   DataModel `json:"data_model"`
}

type Property struct {
  Format string `json:"format"`
  Type   string `json:"type"`
  Command string `json:"$comment"`
}

type DataModel struct {
  ID string `json:"$id"`
  Description string `json:"description"`
  Schema string `json:"schema"`
  Properties map[string]Property `json:"properties"`
}

type SearchClient struct {
  BaseURL string
}

func (td TableDescription) GetInfo() (TableInfo, error) {
  out := TableInfo{}
  client := resty.New()
  resp, err := client.R().
      SetHeader("Accept", "application/json").
      Get(td.DataModel.Ref)
  if err != nil {
      log.Printf("Error: %s", err)
      return out, err
  }
  err = json.Unmarshal(resp.Body(), &out)
  if err != nil {
      log.Printf("Error: %s", err)
      return out, err
  }
  return out, nil
}

func (sc *SearchClient) GetTables() ([]TableDescription, error) {
  client := resty.New()
  out := []TableDescription{}
  for nextURL := sc.BaseURL + "tables"; nextURL != "" ; {
    log.Printf("Getting: %s\n", nextURL)
    resp, err := client.R().
        SetHeader("Accept", "application/json").
        Get(nextURL)
    if err != nil {
      log.Printf("Error %s\n", err)
    }
    tr := TablesResponse{}
    json.Unmarshal(resp.Body(), &tr)
    out = append(out, tr.Tables...)
    if tr.Pagination != nil && tr.Pagination.NextPageURL != "" {
      nextURL = tr.Pagination.NextPageURL
    } else {
      nextURL = ""
    }
  }
  return out, nil
}

type TableData struct {
  DataModel  string `json:"data_model"`
  Data       []map[string]interface{}   `json:"data"`
  Pagination *Pagination `json:"pagination"`
}

func (sc *SearchClient) GetTableRows(name string) (chan map[string]interface{}, error) {
  out := make(chan map[string]interface{}, 100)

  go func() {
    defer close(out)
    client := resty.New()
    startURL := fmt.Sprintf("%stable/%s/data", sc.BaseURL, name)

    for nextURL := startURL; nextURL != "" ; {
      log.Printf("Getting: %s\n", nextURL)
      resp, err := client.R().
          SetHeader("Accept", "application/json").
          Get(nextURL)
      if err != nil {
        log.Printf("Error %s\n", err)
      }
      td := TableData{}
      json.Unmarshal(resp.Body(), &td)
      for _, d := range td.Data {
        out <- d
      }
      if td.Pagination != nil && td.Pagination.NextPageURL != "" {
        nextURL = td.Pagination.NextPageURL
      } else {
        nextURL = ""
      }
    }
  }()
  return out, nil
}


type TableConfig struct {
  PrimaryKey string `json:"primaryKey"`
  Fields     map[string]string `json:"fields"`
}

type Config struct {
  Port    int      `json:"port"`
  BaseURL string   `json:"baseURL"`
  Tables  map[string]TableConfig  `json:"tables"`
}


type GA4GHSearchProxyServer struct {
	gripper.UnimplementedGRIPSourceServer
  client   SearchClient
  Config   Config
}


func (ps *GA4GHSearchProxyServer) GetCollections(n *gripper.Empty, server gripper.GRIPSource_GetCollectionsServer) error {
	for t, c := range ps.Config.Tables {
    if c.PrimaryKey != "" {
		    server.Send(&gripper.Collection{Name: t})
    }
	}
	return nil
}

func (ps *GA4GHSearchProxyServer) GetCollectionInfo(ctx context.Context, col *gripper.Collection) (*gripper.CollectionInfo, error) {
  if tconfig, ok := ps.Config.Tables[col.Name]; ok {
    fields := []string{}
    for k := range tconfig.Fields {
      fields = append(fields, k)
    }
    return &gripper.CollectionInfo{SearchFields:fields}, nil
  }
	return nil, fmt.Errorf("Table %s not found", col.Name)
}

func (ps *GA4GHSearchProxyServer) GetIDs(*gripper.Collection, gripper.GRIPSource_GetIDsServer) error {
	return nil
}

func (ps *GA4GHSearchProxyServer) GetRows(col *gripper.Collection, srv gripper.GRIPSource_GetRowsServer) error {
  if table, ok := ps.Config.Tables[col.Name]; ok {
    rows, err := ps.client.GetTableRows(col.Name)
    if err != nil {
      return err
    }
    for r := range rows {
      if id, ok := r[table.PrimaryKey]; ok {
        if idStr, ok := id.(string); ok {
          s, _ := structpb.NewStruct(r)
          o := gripper.Row{Id:idStr, Data:s}
          srv.Send(&o)
        }
      }
    }
    return nil
  }
  return fmt.Errorf("Table %s not found", col.Name)
}

func (ps *GA4GHSearchProxyServer) GetRowsByID(gripper.GRIPSource_GetRowsByIDServer) error {
	return nil
}

func (ps *GA4GHSearchProxyServer) GetRowsByField(*gripper.FieldRequest, gripper.GRIPSource_GetRowsByFieldServer) error {
	return nil
}

func Serve(cmd *cobra.Command, args []string) error {
  configPath := args[0]
  config := Config{}

  raw, err := ioutil.ReadFile(configPath)
  yaml.Unmarshal(raw, &config)

  if config.Port == 0 {
    config.Port = 50051
  }

	lis, err := net.Listen("tcp", ":" + fmt.Sprintf("%d", config.Port))
	if err != nil {
		return fmt.Errorf("Cannot open port: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxSendMsgSize(1024*1024*16), grpc.MaxRecvMsgSize(1024*1024*16))

  client := SearchClient{config.BaseURL}
	server := GA4GHSearchProxyServer{Config: config, client:client}

	// Regsiter Query Service
	gripper.RegisterGRIPSourceServer(grpcServer, &server)

	fmt.Printf("Starting Server: %d", config.Port)
	err = grpcServer.Serve(lis)
	return err
}


func List(cmd *cobra.Command, args []string) error {
  baseURL := args[0]
  client := SearchClient{baseURL}

  tables, err := client.GetTables()
  if err != nil {
    return err
  }
  for _, t := range tables {
    j, _ := json.Marshal(t)
    fmt.Printf("%s\n", j)
  }
  return nil
}

func GenConfig(cmd *cobra.Command, args []string) error {
  baseURL := args[0]
  client := SearchClient{baseURL}

  out := Config{BaseURL:baseURL, Tables:map[string]TableConfig{}}

  tables, err := client.GetTables()
  if err != nil {
    return err
  }
  for _, t := range tables {
    m, err := t.GetInfo()
    if err == nil {
      tconfig := TableConfig{Fields:map[string]string{}}
      for k, v := range m.DataModel.Properties {
        tconfig.Fields[k] = v.Type
      }
      //guess id field
      idField := ""
      for k := range m.DataModel.Properties {
        if k == "id" {
          idField = k
        }
      }

      if idField == "" {
        log.Printf("Unable to guess field for table %s", t.Name)
      } else {
        tconfig.PrimaryKey = idField
      }
      out.Tables[t.Name] = tconfig
    }
  }
  out.Port = 50051

	y, err := yaml.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(y))

  return nil
}


func main() {
  serverCommand := cobra.Command{
    Use: "server",
    Args: cobra.ExactArgs(1),
    RunE: Serve,
  }

  listCommand := cobra.Command{
    Use: "list",
    Args: cobra.ExactArgs(1),
    RunE: List,
  }

  genCommand := cobra.Command{
    Use: "gen-config",
    Args: cobra.ExactArgs(1),
    RunE: GenConfig,
  }

  rootCmd := &cobra.Command{
    Use: "grip_ga4gh_search",
  }

  rootCmd.AddCommand(&serverCommand)
  rootCmd.AddCommand(&listCommand)
  rootCmd.AddCommand(&genCommand)
  rootCmd.Execute()
}
