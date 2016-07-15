unit sxFailover;

interface

uses
  Windows, Classes, SysUtils, ShellApi, Generics.Collections,
  IdMappedPortTCP, IdTCPClient, IdContext, IdStack, IdTCPConnection, IdYarn, IdGlobal, IdIOHandlerSocket, IdComponent,
  IdIOHandler, DateUtils, SyncObjs, IdSocketHandle, IndyPeerImpl,
  JclSvcCtrl, JclSysInfo;

type
  TsxServerItem = class;

  EsxMappedAttemptException = class(Exception)
  private
    FOriginal: Exception;
  public
    constructor Create(AOriginal: Exception);
    property Original: Exception read FOriginal;
  end;

  TsxServerStatus = (sesActive, sesInactive, sesRestarting);
  TsxVerifyServerEvent = procedure(AItem: TsxServerItem; var AActive: Boolean) of object;
  TsxRestartApplicationEvent = procedure(AItem: TsxServerItem) of object;

  TsxServerItem = class(TCollectionItem)
  private
    FHost: String;
    FPort: Integer;
    FApplication: String;
    FService: Boolean;
    FStatus: TsxServerStatus;
  public
    property Status: TsxServerStatus read FStatus write FStatus;
  published
    property Host: String read FHost write FHost;
    property Port: Integer read FPort write FPort;
    property Application: String read FApplication write FApplication;
    property Service: Boolean read FService write FService;
  end;

  TsxServerList = class(TCollection)
  private
    function GetItem(Index: Integer): TsxServerItem;
    procedure SetItem(Index: Integer; Value: TsxServerItem);
  public
    constructor Create;reintroduce;
    function Add: TsxServerItem;
    property Items[Index: Integer]: TsxServerItem read GetItem write SetItem; default;
  end;

  TIdMappedPortTCPAccess = class(TIdMappedPortTCP);
  TIdIOHandlerAccess = class(TIdIOHandler);
  TIdMappedPortContextAccess = class(TIdMappedPortContext);

  TsxMappedPortContext = class (TIdMappedPortContext)
  protected
    procedure CheckForData(DoRead: Boolean); override;
    procedure OutboundConnect; override;
  public
    constructor Create(AConnection: TIdTCPConnection; AYarn: TIdYarn; AList: TIdContextThreadList = nil); override;
  end;

  TsxVerifyServerThread = class(TThread)
  private
    FWaitTime: Integer;
    FServers: TsxServerList;
    FOnVerify: TsxVerifyServerEvent;
    FOnRestart: TsxRestartApplicationEvent;
    procedure DoVerify(AItem: TsxServerItem; var AActive: Boolean);
    procedure DoRestart(AItem: TsxServerItem);
  protected
    procedure Execute; override;
  public
    constructor Create(AServers: TsxServerList; AWaitTime: Integer);
    property OnVerify: TsxVerifyServerEvent read FOnVerify write FOnVerify;
    property OnRestart: TsxRestartApplicationEvent read FOnRestart write FOnRestart;
  end;

  TpxRestartApplicationThread = class(TThread)
  private
    FConf: TsxServerItem;
    procedure RestartProccess;
    procedure RestartService;
  protected
    constructor Create(AItem: TsxServerItem);
    procedure Execute; override;
  end;

  TsxFailoverTCP = class(TIdMappedPortTCP)
  private
    FServers: TsxServerList;
    FOnVerifyServer: TsxVerifyServerEvent;
    FVerifyServer: TsxVerifyServerThread;
    FVerifyWaitTime: Integer;
    FOnRestartApplication: TsxRestartApplicationEvent;
    procedure SetServers(const Value: TsxServerList);
  protected
    procedure DoConnect(AContext: TIdContext); override;
    procedure Startup; override;
    procedure Shutdown; override;
  public
    procedure InitComponent; override;
    destructor Destroy; override;
  published
    property Servers: TsxServerList read FServers write SetServers;
    property VerifyWaitTime: Integer read FVerifyWaitTime write FVerifyWaitTime default 5000;
    property OnRestartApplication: TsxRestartApplicationEvent read FOnRestartApplication write FOnRestartApplication;
    property OnVerifyServer: TsxVerifyServerEvent read FOnVerifyServer write FOnVerifyServer;
  end;

implementation

{ TsxVerifyServerThread }

constructor TsxVerifyServerThread.Create(AServers: TsxServerList; AWaitTime: Integer);
begin
  inherited Create(True);
  FreeOnTerminate := True;
  FServers := AServers;
  FWaitTime := AWaitTime;
end;

procedure TsxVerifyServerThread.DoRestart(AItem: TsxServerItem);
begin
  if (Assigned(FOnRestart)) then
    FOnRestart(AItem);
end;

procedure TsxVerifyServerThread.DoVerify(AItem: TsxServerItem;var AActive: Boolean);
begin
  if Assigned(FOnVerify) then
    FOnVerify(AItem, AActive);
end;

procedure TsxVerifyServerThread.Execute;
var
  I: Integer;
  AItem: TsxServerItem;
  ARestart: TpxRestartApplicationThread;
  AActive: Boolean;
begin
  inherited;
  while (True) do
  begin
    for I := 0 to FServers.Count -1 do
    begin
      AItem := FServers[I];

      if (Self.Terminated) then
        Exit;

      AActive := False;
      DoVerify(AItem, AActive);

      if (AActive) then
      begin
        TThread.Synchronize(nil, procedure
        begin
          AItem.Status := sesActive;
        end);
      end
      else begin
        if (AItem.Status <> sesRestarting) then
        begin
          TThread.Synchronize(nil, procedure
          begin
            AItem.Status := sesRestarting;
          end);

          DoRestart(AItem);
          ARestart := TpxRestartApplicationThread.Create(AItem);
          ARestart.Start;
        end;
      end;
    end;
    Sleep(FWaitTime);
  end;
end;

{ TpxRestartApplicationThread }

constructor TpxRestartApplicationThread.Create(AItem: TsxServerItem);
begin
  inherited Create(True);
  FreeOnTerminate := True;
  FConf := AItem;
end;

procedure TpxRestartApplicationThread.RestartProccess;
var
  APid: THandle;
begin
  inherited;
  try
    APid := GetPidFromProcessName(ExtractFileName(FConf.Application));
    if (APid <> INVALID_HANDLE_VALUE) then
      TerminateApp(APid, 0);

    ShellExecute(0, nil, PWideChar(FConf.Application), nil, nil, SW_SHOWNORMAL);
  except
    TThread.Synchronize(nil, procedure
    begin
      FConf.Status := sesInactive;
    end);
  end;
end;

procedure TpxRestartApplicationThread.RestartService;
const
  WaitService = 3000;
begin
  StopServiceByName('', FConf.Application);
  Sleep(WaitService);

  StartServiceByName('', FConf.Application);
  Sleep(WaitService);
end;

procedure TpxRestartApplicationThread.Execute;
begin
  if (FConf.Service) then
    RestartService
  else
    RestartProccess;
end;

{ TsxMappedPortContext }

procedure TsxMappedPortContext.CheckForData(DoRead: Boolean);
const
  TIMEOUT_READ = 60000;
var
  FNetDataB: TIdBytes;
begin
  if (not Assigned(FOutboundClient.IOHandler)) then
    Exit;

  //if DoRead then //original: hangs after some traffic in SelectReadList
  if DoRead and
     Connection.IOHandler.InputBufferIsEmpty and
     FOutboundClient.IOHandler.InputBufferIsEmpty then //hanging fix
  begin
    //se nao baixou os dados em 1 minuto está muito lento
    if FReadList.SelectReadList(FDataAvailList, IdTimeoutInfinite) then
    begin
      //1.LConnectionHandle
      if FDataAvailList.ContainsSocket(Connection.Socket.Binding.Handle) then
      begin
        Connection.IOHandler.CheckForDataOnSource(0);
        //Connection.IOHandler.CheckForDataOnSource(TIMEOUT_READ);
        //if (_TIdIOHandler(Connection.IOHandler).Connected) then
          //_TIdIOHandler(Connection.IOHandler).ReadFromSource(True, TIMEOUT_READ, True);
          //_TIdIOHandler(Connection.IOHandler).ReadFromSource(False, TIMEOUT_READ, False);
      end;
      //2.LOutBoundHandle
      if FDataAvailList.ContainsSocket(FOutboundClient.Socket.Binding.Handle) then
      begin
        FOutboundClient.IOHandler.CheckForDataOnSource(0);
        //FOutboundClient.IOHandler.CheckForDataOnSource(TIMEOUT_READ);
        //if (_TIdIOHandler(Connection.IOHandler).Connected) then
          //_TIdIOHandler(Connection.IOHandler).ReadFromSource(True, TIMEOUT_READ, True);
          //_TIdIOHandler(Connection.IOHandler).ReadFromSource(False, TIMEOUT_READ, False);
      end;
    end;
  end;
  if not Connection.IOHandler.InputBufferIsEmpty then
  begin
    SetLength(FNetDataB, 0);
    Connection.IOHandler.InputBuffer.ExtractToBytes(FNetDataB);
    TIdMappedPortTCPAccess(Server).DoLocalClientData(Self);
    FOutboundClient.IOHandler.Write(FNetDataB);
  end;
  if not FOutboundClient.IOHandler.InputBufferIsEmpty then
  begin
     SetLength(FNetDataB, 0);
     FOutboundClient.IOHandler.InputBuffer.ExtractToBytes(FNetDataB);
     TIdMappedPortTCPAccess(Server).DoOutboundClientData(Self);
     Connection.IOHandler.Write(FNetDataB);
  end;
end;

constructor TsxMappedPortContext.Create(AConnection: TIdTCPConnection; AYarn: TIdYarn; AList: TIdContextThreadList);
begin
  inherited Create(AConnection, AYarn, AList);
  FConnectTimeOut := 3000;
end;

procedure TsxMappedPortContext.OutboundConnect;
const
  MaxReconnectAttempts = 5;
var
  AAttempt: Integer;
  ATCPClient: TIdTCPClient;
  AServer: TsxFailoverTCP;
begin
  //inherited;

  AAttempt := 0;
  while (True) do
  begin
    AServer := TsxFailoverTCP(Server);
    try
      ATCPClient := TIdTCPClient.Create(nil);
      FOutboundClient := ATCPClient;

      ATCPClient.Port := AServer.MappedPort;
      ATCPClient.Host := AServer.MappedHost;

      AServer.DoLocalClientConnect(Self);

      ATCPClient.ConnectTimeout := Self.ConnectTimeOut;
      ATCPClient.Connect;

      AServer.DoOutboundClientConnect(Self);
      //APR: buffer can contain data from prev (users) read op.
      CheckForData(False);
      Break;
    except
      on E: Exception do
      begin
        Inc(AAttempt);
        if (AAttempt < MaxReconnectAttempts) then
        begin
          DoException(E);
          Connection.Disconnect;
        end
        else begin
          DoException(EsxMappedAttemptException.Create(E));
          Connection.Disconnect;
          raise;
        end;
      end;
    end;
  end;
end;

{ TsxServerList }

constructor TsxServerList.Create;
begin
  inherited Create(TsxServerItem);
end;

function TsxServerList.Add: TsxServerItem;
begin
  Result := TsxServerItem(inherited Add);
end;

function TsxServerList.GetItem(Index: Integer): TsxServerItem;
begin
  Result := TsxServerItem(inherited GetItem(Index));
end;

procedure TsxServerList.SetItem(Index: Integer; Value: TsxServerItem);
begin
  inherited SetItem(Index, Value);
end;

{ TsxFailoverTCP }

procedure TsxFailoverTCP.InitComponent;
begin
  inherited InitComponent;
  FContextClass := TsxMappedPortContext;
  FServers := TsxServerList.Create;
  FVerifyWaitTime := 5000;
end;

destructor TsxFailoverTCP.Destroy;
begin
  FServers.Free;
  inherited Destroy;
end;

procedure TsxFailoverTCP.Startup;
begin
  inherited Startup;
  FVerifyServer := TsxVerifyServerThread.Create(FServers, FVerifyWaitTime);
  FVerifyServer.OnVerify := FOnVerifyServer;
  FVerifyServer.OnRestart := FOnRestartApplication;
  FVerifyServer.Start;
end;

procedure TsxFailoverTCP.Shutdown;
begin
  FVerifyServer.Terminate;
  inherited Shutdown;
end;

procedure TsxFailoverTCP.SetServers(const Value: TsxServerList);
begin
  FServers.Assign(Value);
end;

procedure TsxFailoverTCP.DoConnect(AContext: TIdContext);
begin
  inherited;
  DoBeforeConnect(AContext);

  //WARNING: Check TIdTCPServer.DoConnect and synchronize code. Don't call inherited!=> OnConnect in OutboundConnect    {Do not Localize}
  TIdMappedPortContextAccess(AContext).OutboundConnect;

  //cache
  if (not Assigned(TIdMappedPortContextAccess(AContext).FOutboundClient.IOHandler)) then
    Exit;

  with TIdMappedPortContextAccess(AContext).FReadList do begin
    Clear;
    Add((AContext.Connection.IOHandler as TIdIOHandlerSocket).Binding.Handle);
    Add((TIdMappedPortContextAccess(AContext).FOutboundClient.IOHandler as TIdIOHandlerSocket).Binding.Handle);
  end;
end;

{ EsxMappedAttemptException }

constructor EsxMappedAttemptException.Create(AOriginal: Exception);
begin
  inherited Create(AOriginal.Message);
  FOriginal := AOriginal;
end;

end.
