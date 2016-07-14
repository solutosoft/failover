unit sxFailoverReg;

interface

uses
  Classes, sxFailover;

procedure Register;

implementation

procedure Register;
begin
  RegisterComponents('Failover', [TsxFailoverTCP]);
end;


end.
