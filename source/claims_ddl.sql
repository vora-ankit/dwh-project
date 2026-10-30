-- dbo.Claims: cleaned for Azure SQL Database (single batch, no GO, no filegroups).
-- Changes from the original script:
--   * user-defined types replaced with base types
--       Id_t -> INT, Code_t -> VARCHAR(20), Date_t -> DATETIME2(0), Money_t -> DECIMAL(18,2),
--       YesNo_t -> CHAR(1), Name_t -> VARCHAR(100), ClaimNumber_t -> VARCHAR(30), REFNumber_t -> VARCHAR(50)
--   * duplicate InitialAdjudicationVersion column removed
--   * ClaimID is IDENTITY so the generator does not have to assign keys
--   * TIMESTAMP spelled ROWVERSION (same type, clearer name)
--   * defaults declared inline as named constraints
IF OBJECT_ID('dbo.Claims', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Claims (
        ClaimID                          INT IDENTITY(1,1) NOT NULL,
        AdjustmentVersion                INT NULL,
        ClaimNumber                      VARCHAR(30) NULL,
        FormType                         VARCHAR(20) NULL,
        ClaimType                        VARCHAR(20) NULL,
        ServiceDateFrom                  DATETIME2(0) NULL,
        ServiceDateTo                    DATETIME2(0) NULL,
        DateReceived                     DATETIME2(0) NULL,
        DatePaid                         DATETIME2(0) NULL,
        MemberId                         INT NULL,
        MemberCoverageId                 INT NULL,
        SubscriberContractId             INT NULL,
        SubscriberId                     INT NULL,
        GroupId                          INT NULL,
        ProviderId                       INT NULL,
        OfficeId                         INT NULL,
        VendorId                         INT NULL,
        Status                           VARCHAR(20) NULL,
        OutstandingMemberBalance         CHAR(1) NULL,
        ExternalClaimNumber              VARCHAR(50) NULL,
        InputBatchID                     INT NULL,
        LastOutputBatchID                INT NULL,
        OriginalStatus                   VARCHAR(20) NULL,
        AutoTriagedOnInput               CHAR(1) NULL,
        Verifier                         VARCHAR(100) NULL,
        DateVerified                     DATETIME2(0) NULL,
        DocumentID                       INT NULL,
        Priority                         REAL NULL,
        UtilizationDate                  DATETIME2(0) NULL,
        InvoiceID                        INT NULL,
        ClosedDate                       DATETIME2(0) NULL,
        PayMember                        CHAR(1) NULL,
        PayProvider                      CHAR(1) NULL,
        DateEOBPrinted                   DATETIME2(0) NULL,
        DateEOPPrinted                   DATETIME2(0) NULL,
        DateESTPrinted                   DATETIME2(0) NULL,
        Date837Exported                  DATETIME2(0) NULL,
        DateEncounterExported            DATETIME2(0) NULL,
        LastPolledAt                     DATETIME2(0) NOT NULL
            CONSTRAINT DF_Claims_LastPolledAt DEFAULT ('1900-01-01'),
        AmtNegotiated                    DECIMAL(18,2) NULL,
        AmtMaximumAllowable              DECIMAL(18,2) NULL,
        LastUpdatedBy                    INT NULL,
        LastUpdatedAt                    DATETIME2(0) NULL
            CONSTRAINT DF_Claims_LastUpdatedAt DEFAULT (GETDATE()),
        LastArchivedAt                   INT NULL
            CONSTRAINT DF_Claims_LastArchivedAt DEFAULT (0),
        SequenceByDateReceived           VARCHAR(30) NULL,
        DatePrintedByTriage              DATETIME2(0) NULL,
        ProcessingStatus                 VARCHAR(20) NULL
            CONSTRAINT DF_Claims_ProcessingStatus DEFAULT (''),
        DateCoverSheetPrinted            DATETIME2(0) NULL,
        EntryDateTime                    DATETIME2(0) NULL
            CONSTRAINT DF_Claims_EntryDateTime DEFAULT (GETDATE()),
        CleanedDate                      DATETIME2(0) NULL,
        AdjustedClosedDate               DATETIME2(0) NULL,
        ClosedVersion                    INT NULL,
        InitialAdjudicationVersion       INT NULL,
        AdjudicationActionCodes          CHAR(1) NULL,
        PaymentActionCodes               CHAR(1) NULL,
        Category1                        CHAR(1) NULL,
        BilledCurrency                   VARCHAR(20) NULL,
        PaymentClass                     VARCHAR(20) NULL,
        ClaimLoadID                      INT NULL,
        ExplanationID                    INT NULL,
        TypeOfBill                       VARCHAR(5) NULL,
        AssignmentBenefits               VARCHAR(2) NULL,
        PreestimateMatchingExplanationID INT NULL,
        EOBAction                        VARCHAR(20) NOT NULL
            CONSTRAINT DF_EOBAction DEFAULT ('SYS'),
        AdjustmentReasonCode             VARCHAR(20) NULL,
        ClaimCategoryCode                VARCHAR(20) NULL,
        InitialAdjudicationDate          DATETIME2(0) NULL,
        InitialReadyToCloseDate          DATETIME2(0) NULL,
        FormSubtype                      VARCHAR(20) NULL,
        LastActiveRowVersionNumber       ROWVERSION NOT NULL,
        ClaimEditingService              VARCHAR(20) NULL,
        CONSTRAINT PK_Claims PRIMARY KEY CLUSTERED (ClaimID ASC)
    );
END
