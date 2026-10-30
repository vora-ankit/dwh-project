-- dbo.Providers and dbo.Members: synthetic dimension sources for the claims warehouse.
-- Single batch, no GO, safe to rerun. Every table has a ROWVERSION column used as the
-- incremental watermark by extract_tables.py.

IF OBJECT_ID('dbo.Providers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Providers (
        ProviderID        INT IDENTITY(1,1) NOT NULL,
        NPI               CHAR(10) NOT NULL,
        ProviderName      VARCHAR(100) NOT NULL,
        ProviderType      VARCHAR(20) NOT NULL,      -- INDIVIDUAL or FACILITY
        Specialty         VARCHAR(50) NULL,
        City              VARCHAR(50) NULL,
        State             CHAR(2) NULL,
        PostalCode        VARCHAR(10) NULL,
        NetworkStatus     VARCHAR(10) NOT NULL,      -- IN or OUT
        ContractStartDate DATE NULL,
        IsActive          BIT NOT NULL
            CONSTRAINT DF_Providers_IsActive DEFAULT (1),
        LastUpdatedAt     DATETIME2(0) NOT NULL
            CONSTRAINT DF_Providers_LastUpdatedAt DEFAULT (SYSUTCDATETIME()),
        RowVersionNumber  ROWVERSION NOT NULL,
        CONSTRAINT PK_Providers PRIMARY KEY CLUSTERED (ProviderID)
    );
END;

IF OBJECT_ID('dbo.Members', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Members (
        MemberID          INT IDENTITY(1,1) NOT NULL,
        FirstName         VARCHAR(50) NOT NULL,
        LastName          VARCHAR(50) NOT NULL,
        DateOfBirth       DATE NOT NULL,
        Gender            CHAR(1) NULL,
        State             CHAR(2) NULL,
        PostalCode        VARCHAR(10) NULL,
        PlanType          VARCHAR(10) NOT NULL,      -- HMO, PPO, EPO, HDHP
        GroupID           INT NULL,
        EffectiveDate     DATE NOT NULL,
        TerminationDate   DATE NULL,
        LastUpdatedAt     DATETIME2(0) NOT NULL
            CONSTRAINT DF_Members_LastUpdatedAt DEFAULT (SYSUTCDATETIME()),
        RowVersionNumber  ROWVERSION NOT NULL,
        CONSTRAINT PK_Members PRIMARY KEY CLUSTERED (MemberID)
    );
END;
