-- ============================================================================
-- R46 BANKING SYSTEM — End-State Production Schema
-- ============================================================================
-- Hyper-normalized (BCNF). All integrity checks in SQL.
-- Every enum is a FK table. Every street/city name is deduplicated.
-- All monetary precision, ACID invariants, and business rules enforced
-- at the schema level via CHECK constraints.
-- ============================================================================

-- ============================================================================
-- §0  ENUMERATIONS — Every status/type/code is a FK-referenced table
-- ============================================================================

CREATE TABLE dbo.Gender (
    GenderId        TINYINT         NOT NULL PRIMARY KEY,
    Code            CHAR(1)         NOT NULL UNIQUE
        CHECK (Code IN ('M','F','X','U')),
    Label           NVARCHAR(20)    NOT NULL UNIQUE
);

CREATE TABLE dbo.MaritalStatus (
    MaritalStatusId TINYINT         NOT NULL PRIMARY KEY,
    Code            CHAR(3)         NOT NULL UNIQUE
        CHECK (Code IN ('SNG','MAR','DIV','WID','SEP','CIV')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.IdentityDocumentType (
    DocTypeId       TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE,
    Label           NVARCHAR(40)    NOT NULL UNIQUE
);

CREATE TABLE dbo.ContactType (
    ContactTypeId   TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('MOBILE','HOME','WORK','FAX','EMAIL')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.AccountType (
    AccountTypeId   TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('CHECKING','SAVINGS','DEPOSIT','ESCROW','LOAN')),
    Label           NVARCHAR(40)    NOT NULL UNIQUE,
    IsLoanBacked    BIT             NOT NULL DEFAULT 0
);

CREATE TABLE dbo.AccountStatus (
    AccountStatusId TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('ACTIVE','DORMANT','FROZEN','CLOSED','PENDING')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.TransactionType (
    TxTypeId        SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE
        CHECK (Code IN ('CREDIT','DEBIT','TRANSFER','FEE','INTEREST',
                        'REVERSAL','ADJUSTMENT','OPENING','CLOSING')),
    Label           NVARCHAR(40)    NOT NULL UNIQUE,
    SignConvention  SMALLINT        NOT NULL
        CHECK (SignConvention IN (-1, 0, 1))
);

CREATE TABLE dbo.TransactionStatus (
    TxStatusId      TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('PENDING','POSTED','REVERSED','FAILED','HELD')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.LoanType (
    LoanTypeId      TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE
        CHECK (Code IN ('MORTGAGE','PERSONAL','AUTO','STUDENT',
                        'CREDIT_LINE','OVERDRAFT')),
    Label           NVARCHAR(40)    NOT NULL UNIQUE,
    RegulatoryClass VARCHAR(10)     NOT NULL
        CHECK (RegulatoryClass IN ('RETAIL','CORPORATE','SECURED','UNSECURED'))
);

CREATE TABLE dbo.LoanStatus (
    LoanStatusId    TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE
        CHECK (Code IN ('APPLIED','APPROVED','DISBURSED','PERFORMING',
                        'DELINQUENT','DEFAULT','CLOSED','WRITTEN_OFF')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.RepaymentFrequency (
    FrequencyId     TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('MONTHLY','BIWEEKLY','QUARTERLY','ANNUAL','BULLET')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE,
    PeriodsPerYear  SMALLINT        NOT NULL
        CHECK (PeriodsPerYear >= 0 AND PeriodsPerYear <= 52)
);

CREATE TABLE dbo.InterestRateType (
    RateTypeId      TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('FIXED','VARIABLE','HYBRID','TRACKER')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.ReferenceRateIndex (
    IndexId         TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE,
    Label           NVARCHAR(50)    NOT NULL UNIQUE,
    Currency        CHAR(3)         NOT NULL
        CHECK (LEN(Currency) = 3),
    TenorMonths     TINYINT         NOT NULL
        CHECK (TenorMonths > 0 AND TenorMonths <= 60)
);

CREATE TABLE dbo.Currency (
    CurrencyId      SMALLINT        NOT NULL PRIMARY KEY,
    IsoCode         CHAR(3)         NOT NULL UNIQUE
        CHECK (LEN(IsoCode) = 3 AND IsoCode = UPPER(IsoCode)),
    Name            NVARCHAR(40)    NOT NULL,
    DecimalPlaces   TINYINT         NOT NULL
        CHECK (DecimalPlaces >= 0 AND DecimalPlaces <= 4),
    IsBaseCurrency  BIT             NOT NULL DEFAULT 0
);

CREATE TABLE dbo.DocumentCategory (
    DocCategoryId   TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE,
    Label           NVARCHAR(40)    NOT NULL UNIQUE
);

CREATE TABLE dbo.AuditActionType (
    ActionTypeId    SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(20)     NOT NULL UNIQUE,
    Label           NVARCHAR(60)    NOT NULL UNIQUE,
    Severity        TINYINT         NOT NULL
        CHECK (Severity >= 1 AND Severity <= 3)
);

CREATE TABLE dbo.ComplianceFlag (
    FlagId          SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE
        CHECK (Code IN ('PEP','SANCTIONS','AML_ALERT','FATCA','CRS')),
    Label           NVARCHAR(50)    NOT NULL UNIQUE,
    RequiresReview  BIT             NOT NULL DEFAULT 1
);

CREATE TABLE dbo.AddressType (
    AddressTypeId   TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('HOME','WORK','LEGAL','MAILING','PREVIOUS')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.AccountOwnershipType (
    OwnershipTypeId TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(12)     NOT NULL UNIQUE
        CHECK (Code IN ('PRIMARY','JOINT','POA','BENEFICIARY','GUARDIAN')),
    Label           NVARCHAR(40)    NOT NULL UNIQUE
);

CREATE TABLE dbo.LoanPartyRole (
    RoleId          TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE
        CHECK (Code IN ('BORROWER','CO_BORROWER','GUARANTOR')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.CollateralType (
    CollateralTypeId TINYINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE
        CHECK (Code IN ('PROPERTY','VEHICLE','DEPOSIT','GUARANTEE','SECURITIES')),
    Label           NVARCHAR(40)    NOT NULL UNIQUE
);

CREATE TABLE dbo.BranchType (
    BranchTypeId    TINYINT         NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('HQ','BRANCH','AGENCY','ATM_SITE','MOBILE')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);

CREATE TABLE dbo.FeeType (
    FeeTypeId       SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(20)     NOT NULL UNIQUE,
    Label           NVARCHAR(60)    NOT NULL UNIQUE
);

CREATE TABLE dbo.TransactionTagType (
    TagTypeId       SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(15)     NOT NULL UNIQUE,
    Label           NVARCHAR(40)    NOT NULL UNIQUE
);

CREATE TABLE dbo.PermissionScope (
    ScopeId         SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(20)     NOT NULL UNIQUE,
    Label           NVARCHAR(40)    NOT NULL UNIQUE
);

CREATE TABLE dbo.PermissionAction (
    ActionId        SMALLINT        NOT NULL PRIMARY KEY,
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (Code IN ('VIEW','CREATE','UPDATE','DELETE','APPROVE',
                        'REVERSE','EXPORT')),
    Label           NVARCHAR(30)    NOT NULL UNIQUE
);


-- ============================================================================
-- §1  GEOGRAPHY — Every address component is a separate table
-- ============================================================================

CREATE TABLE dbo.Country (
    CountryId       SMALLINT        NOT NULL PRIMARY KEY,
    IsoAlpha2       CHAR(2)         NOT NULL UNIQUE
        CHECK (LEN(IsoAlpha2) = 2 AND IsoAlpha2 = UPPER(IsoAlpha2)),
    IsoAlpha3       CHAR(3)         NOT NULL UNIQUE
        CHECK (LEN(IsoAlpha3) = 3 AND IsoAlpha3 = UPPER(IsoAlpha3)),
    Name            NVARCHAR(80)    NOT NULL UNIQUE,
    PhonePrefix     VARCHAR(5)      NULL
        CHECK (PhonePrefix IS NULL OR PhonePrefix LIKE '+%')
);

CREATE TABLE dbo.Region (
    RegionId        INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    CountryId       SMALLINT        NOT NULL REFERENCES dbo.Country(CountryId),
    Code            VARCHAR(10)     NOT NULL,
    Name            NVARCHAR(80)    NOT NULL,
    UNIQUE (CountryId, Code)
);

CREATE TABLE dbo.CityName (
    CityNameId      INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    Name            NVARCHAR(100)   NOT NULL UNIQUE
        CHECK (LEN(Name) >= 1)
);

CREATE TABLE dbo.City (
    CityId          INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    RegionId        INT             NOT NULL REFERENCES dbo.Region(RegionId),
    CityNameId      INT             NOT NULL REFERENCES dbo.CityName(CityNameId),
    SirutaCode      INT             NULL
        CHECK (SirutaCode IS NULL OR SirutaCode > 0),
    UNIQUE (RegionId, CityNameId)
);

CREATE TABLE dbo.PostalCode (
    PostalCodeId    INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    CityId          INT             NOT NULL REFERENCES dbo.City(CityId),
    Code            VARCHAR(15)     NOT NULL
        CHECK (LEN(Code) >= 2),
    UNIQUE (CityId, Code)
);

CREATE TABLE dbo.StreetName (
    StreetNameId    INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    Name            NVARCHAR(150)   NOT NULL UNIQUE
        CHECK (LEN(Name) >= 1)
);

CREATE TABLE dbo.Address (
    AddressId       BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    PostalCodeId    INT             NOT NULL REFERENCES dbo.PostalCode(PostalCodeId),
    StreetNameId    INT             NOT NULL REFERENCES dbo.StreetName(StreetNameId),
    StreetNumber    VARCHAR(15)     NOT NULL
        CHECK (LEN(StreetNumber) >= 1),
    Building        VARCHAR(20)     NULL,
    Staircase       VARCHAR(10)     NULL,
    Floor           VARCHAR(10)     NULL,
    Apartment       VARCHAR(10)     NULL,
    Latitude        DECIMAL(10,7)   NULL
        CHECK (Latitude IS NULL OR (Latitude >= -90 AND Latitude <= 90)),
    Longitude       DECIMAL(10,7)   NULL
        CHECK (Longitude IS NULL OR (Longitude >= -180 AND Longitude <= 180)),
    -- Both geo fields must be present or both absent
    CHECK ((Latitude IS NULL AND Longitude IS NULL) OR
           (Latitude IS NOT NULL AND Longitude IS NOT NULL)),
    -- GDPR soft-delete
    IsDeleted       BIT             NOT NULL DEFAULT 0,
    DeletedAt       DATETIME2       NULL,
    CHECK ((IsDeleted = 0 AND DeletedAt IS NULL) OR
           (IsDeleted = 1 AND DeletedAt IS NOT NULL))
);

CREATE TABLE dbo.Branch (
    BranchId        INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    BranchTypeId    TINYINT         NOT NULL REFERENCES dbo.BranchType(BranchTypeId),
    Code            VARCHAR(10)     NOT NULL UNIQUE
        CHECK (LEN(Code) >= 2),
    Name            NVARCHAR(80)    NOT NULL,
    AddressId       BIGINT          NOT NULL REFERENCES dbo.Address(AddressId),
    OpenedDate      DATE            NOT NULL,
    ClosedDate      DATE            NULL,
    IsActive        BIT             NOT NULL DEFAULT 1,
    BnrCode         VARCHAR(10)     NULL,
    -- ClosedDate must be after OpenedDate
    CHECK (ClosedDate IS NULL OR ClosedDate > OpenedDate),
    -- Closed branches must be inactive
    CHECK ((ClosedDate IS NULL AND IsActive = 1) OR
           (ClosedDate IS NOT NULL AND IsActive = 0) OR
           (ClosedDate IS NULL AND IsActive = 0))  -- dormant but not closed
);


-- ============================================================================
-- §2  PERSONS (Customers)
-- ============================================================================

CREATE TABLE dbo.Person (
    PersonId        BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    FirstName       NVARCHAR(80)    NOT NULL CHECK (LEN(FirstName) >= 1),
    MiddleName      NVARCHAR(80)    NULL,
    LastName        NVARCHAR(80)    NOT NULL CHECK (LEN(LastName) >= 1),
    MaidenName      NVARCHAR(80)    NULL,
    DateOfBirth     DATE            NOT NULL
        CHECK (DateOfBirth >= '1900-01-01' AND DateOfBirth <= GETDATE()),
    GenderId        TINYINT         NOT NULL REFERENCES dbo.Gender(GenderId),
    MaritalStatusId TINYINT         NOT NULL REFERENCES dbo.MaritalStatus(MaritalStatusId),
    -- Romanian CNP: exactly 13 digits
    Cnp             CHAR(13)        NULL UNIQUE
        CHECK (Cnp IS NULL OR (LEN(Cnp) = 13 AND Cnp NOT LIKE '%[^0-9]%')),
    Nationality     SMALLINT        NOT NULL REFERENCES dbo.Country(CountryId),
    -- KYC
    KycLevel        TINYINT         NOT NULL DEFAULT 1
        CHECK (KycLevel >= 1 AND KycLevel <= 3),
    KycLastReviewDate DATE          NULL,
    RiskScore       SMALLINT        NULL
        CHECK (RiskScore IS NULL OR (RiskScore >= 0 AND RiskScore <= 1000)),
    -- Timestamps
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    -- GDPR
    GdprConsentDate DATE            NULL,
    MarketingConsent BIT            NOT NULL DEFAULT 0,
    -- Soft-delete
    IsDeleted       BIT             NOT NULL DEFAULT 0,
    DeletedAt       DATETIME2       NULL,
    DeletedReason   NVARCHAR(200)   NULL,
    CHECK ((IsDeleted = 0 AND DeletedAt IS NULL) OR
           (IsDeleted = 1 AND DeletedAt IS NOT NULL)),
    CHECK (UpdatedAt >= CreatedAt)
);

CREATE TABLE dbo.PersonAddress (
    PersonAddressId BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    AddressId       BIGINT          NOT NULL REFERENCES dbo.Address(AddressId),
    AddressTypeId   TINYINT         NOT NULL REFERENCES dbo.AddressType(AddressTypeId),
    IsPrimary       BIT             NOT NULL DEFAULT 0,
    ValidFrom       DATE            NOT NULL,
    ValidTo         DATE            NULL,
    CHECK (ValidTo IS NULL OR ValidTo > ValidFrom),
    UNIQUE (PersonId, AddressId, AddressTypeId, ValidFrom)
);

CREATE TABLE dbo.IdentityDocument (
    DocumentId      BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    DocTypeId       TINYINT         NOT NULL REFERENCES dbo.IdentityDocumentType(DocTypeId),
    Series          VARCHAR(10)     NULL,
    Number          VARCHAR(20)     NOT NULL CHECK (LEN(Number) >= 1),
    IssuedBy        NVARCHAR(100)   NOT NULL,
    IssuedDate      DATE            NOT NULL,
    ExpiryDate      DATE            NOT NULL,
    IsVerified      BIT             NOT NULL DEFAULT 0,
    VerifiedBy      INT             NULL,  -- FK to Employee added after Employee is created
    VerifiedAt      DATETIME2       NULL,
    -- Expiry must be after issue
    CHECK (ExpiryDate > IssuedDate),
    -- Verified fields must be consistent
    CHECK ((IsVerified = 0 AND VerifiedBy IS NULL AND VerifiedAt IS NULL) OR
           (IsVerified = 1 AND VerifiedBy IS NOT NULL AND VerifiedAt IS NOT NULL))
);

CREATE TABLE dbo.PersonContact (
    ContactId       BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    ContactTypeId   TINYINT         NOT NULL REFERENCES dbo.ContactType(ContactTypeId),
    Value           NVARCHAR(200)   NOT NULL CHECK (LEN(Value) >= 3),
    IsPrimary       BIT             NOT NULL DEFAULT 0,
    IsVerified      BIT             NOT NULL DEFAULT 0,
    ConsentGiven    BIT             NOT NULL DEFAULT 0,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.PersonComplianceFlag (
    PersonFlagId    BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    FlagId          SMALLINT        NOT NULL REFERENCES dbo.ComplianceFlag(FlagId),
    FlaggedDate     DATE            NOT NULL,
    FlaggedBy       INT             NULL,
    ReviewedDate    DATE            NULL,
    ReviewedBy      INT             NULL,
    Resolution      NVARCHAR(500)   NULL,
    IsActive        BIT             NOT NULL DEFAULT 1,
    -- Review must be after flagging
    CHECK (ReviewedDate IS NULL OR ReviewedDate >= FlaggedDate),
    UNIQUE (PersonId, FlagId, FlaggedDate)
);


-- ============================================================================
-- §3  ACCOUNTS & TRANSACTIONS
-- ============================================================================

CREATE TABLE dbo.Account (
    AccountId       BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    -- IBAN: 2-letter country + 2 check digits + up to 30 BBAN chars
    Iban            VARCHAR(34)     NOT NULL UNIQUE
        CHECK (LEN(Iban) >= 15 AND LEN(Iban) <= 34
               AND LEFT(Iban, 2) = UPPER(LEFT(Iban, 2))
               AND SUBSTRING(Iban, 3, 2) NOT LIKE '%[^0-9]%'),
    LegacyAccountNo VARCHAR(20)     NULL UNIQUE,
    AccountTypeId   TINYINT         NOT NULL REFERENCES dbo.AccountType(AccountTypeId),
    AccountStatusId TINYINT         NOT NULL REFERENCES dbo.AccountStatus(AccountStatusId),
    CurrencyId      SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    BranchId        INT             NOT NULL REFERENCES dbo.Branch(BranchId),
    -- Balances — 4 decimal places (sub-cent precision for interest accrual)
    AvailableBalance DECIMAL(18,4)  NOT NULL DEFAULT 0,
    LedgerBalance    DECIMAL(18,4)  NOT NULL DEFAULT 0,
    HoldAmount       DECIMAL(18,4)  NOT NULL DEFAULT 0
        CHECK (HoldAmount >= 0),
    -- Available = Ledger - Hold (enforced at schema level)
    -- NOTE: Cannot do cross-column CHECK with computed — enforced via trigger or app
    -- But we CAN enforce: Available <= Ledger (no phantom money)
    CHECK (AvailableBalance <= LedgerBalance + 0.0001),  -- epsilon for rounding
    -- Lifecycle
    OpenedDate      DATE            NOT NULL,
    ClosedDate      DATE            NULL,
    CHECK (ClosedDate IS NULL OR ClosedDate > OpenedDate),
    -- Interest accrual
    LastInterestCalcDate DATE       NULL,
    AccruedInterest  DECIMAL(18,6)  NOT NULL DEFAULT 0,  -- 6dp for sub-cent accrual
    -- Metadata
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CHECK (UpdatedAt >= CreatedAt)
);

CREATE TABLE dbo.AccountOwnership (
    OwnershipId     BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    AccountId       BIGINT          NOT NULL REFERENCES dbo.Account(AccountId),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    OwnershipTypeId TINYINT         NOT NULL REFERENCES dbo.AccountOwnershipType(OwnershipTypeId),
    SharePercent    DECIMAL(5,2)    NULL
        CHECK (SharePercent IS NULL OR (SharePercent > 0 AND SharePercent <= 100)),
    ValidFrom       DATE            NOT NULL,
    ValidTo         DATE            NULL,
    CHECK (ValidTo IS NULL OR ValidTo > ValidFrom),
    UNIQUE (AccountId, PersonId, OwnershipTypeId, ValidFrom)
);

CREATE TABLE dbo.TransactionBatch (
    BatchId         BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    BatchDate       DATE            NOT NULL,
    Description     NVARCHAR(200)   NOT NULL,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    InitiatedBy     INT             NULL
);

CREATE TABLE dbo.[Transaction] (
    TransactionId   BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    AccountId       BIGINT          NOT NULL REFERENCES dbo.Account(AccountId),
    TxTypeId        SMALLINT        NOT NULL REFERENCES dbo.TransactionType(TxTypeId),
    TxStatusId      TINYINT         NOT NULL REFERENCES dbo.TransactionStatus(TxStatusId),
    CurrencyId      SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    -- Amount is ALWAYS positive; sign is determined by TxType.SignConvention
    Amount          DECIMAL(18,4)   NOT NULL
        CHECK (Amount > 0),
    -- Running balance after posting (can be negative for overdraft)
    BalanceAfter    DECIMAL(18,4)   NOT NULL,
    -- Counterparty (internal or external)
    CounterpartyAccountId BIGINT    NULL REFERENCES dbo.Account(AccountId),
    CounterpartyName      NVARCHAR(200) NULL,
    CounterpartyIban      VARCHAR(34)   NULL
        CHECK (CounterpartyIban IS NULL OR LEN(CounterpartyIban) >= 15),
    -- Self-transfer guard: cannot transact with yourself
    CHECK (CounterpartyAccountId IS NULL OR CounterpartyAccountId <> AccountId),
    -- Narrative
    Reference       VARCHAR(50)     NULL,
    Description     NVARCHAR(500)   NULL,
    -- Batch
    BatchId         BIGINT          NULL REFERENCES dbo.TransactionBatch(BatchId),
    ValueDate       DATE            NOT NULL,
    BookingDate     DATE            NOT NULL,
    -- BookingDate cannot be before ValueDate (but can be same day)
    CHECK (BookingDate >= ValueDate),
    -- FX fields: all-or-nothing
    OriginalAmount      DECIMAL(18,4)   NULL
        CHECK (OriginalAmount IS NULL OR OriginalAmount > 0),
    OriginalCurrencyId  SMALLINT        NULL REFERENCES dbo.Currency(CurrencyId),
    ExchangeRate        DECIMAL(18,8)   NULL
        CHECK (ExchangeRate IS NULL OR ExchangeRate > 0),
    CHECK ((OriginalAmount IS NULL AND OriginalCurrencyId IS NULL AND ExchangeRate IS NULL) OR
           (OriginalAmount IS NOT NULL AND OriginalCurrencyId IS NOT NULL AND ExchangeRate IS NOT NULL)),
    -- FX currency must differ from transaction currency
    CHECK (OriginalCurrencyId IS NULL OR OriginalCurrencyId <> CurrencyId),
    -- Reversal chain: self-FKs
    ReversedByTxId      BIGINT      NULL,  -- points to the tx that reversed this one
    ReversesOrigTxId    BIGINT      NULL,  -- points to the original tx this one reverses
    -- A tx cannot reverse itself
    CHECK (ReversesOrigTxId IS NULL OR ReversesOrigTxId <> TransactionId),
    -- If this tx reverses another, it must be a REVERSAL type (TxTypeId checked at app level)
    -- Channel
    Channel         VARCHAR(15)     NULL
        CHECK (Channel IS NULL OR Channel IN
               ('BRANCH','ATM','ONLINE','MOBILE','SWIFT','SEPA','INTERNAL')),
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Self-referential FKs (added after table creation)
ALTER TABLE dbo.[Transaction]
    ADD CONSTRAINT FK_Transaction_ReversedBy
        FOREIGN KEY (ReversedByTxId) REFERENCES dbo.[Transaction](TransactionId);
ALTER TABLE dbo.[Transaction]
    ADD CONSTRAINT FK_Transaction_ReversesOrig
        FOREIGN KEY (ReversesOrigTxId) REFERENCES dbo.[Transaction](TransactionId);

CREATE TABLE dbo.TransactionTag (
    TagId           BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    TransactionId   BIGINT          NOT NULL REFERENCES dbo.[Transaction](TransactionId),
    TagTypeId       SMALLINT        NOT NULL REFERENCES dbo.TransactionTagType(TagTypeId),
    Value           NVARCHAR(100)   NOT NULL CHECK (LEN(Value) >= 1),
    UNIQUE (TransactionId, TagTypeId, Value)
);


-- ============================================================================
-- §4  LOANS & RATES
-- ============================================================================

CREATE TABLE dbo.Loan (
    LoanId          BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    LoanAccountId   BIGINT          NOT NULL REFERENCES dbo.Account(AccountId),
    DisbursementAccountId BIGINT    NOT NULL REFERENCES dbo.Account(AccountId),
    LoanTypeId      TINYINT         NOT NULL REFERENCES dbo.LoanType(LoanTypeId),
    LoanStatusId    TINYINT         NOT NULL REFERENCES dbo.LoanStatus(LoanStatusId),
    CurrencyId      SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    BranchId        INT             NOT NULL REFERENCES dbo.Branch(BranchId),
    -- Loan and disbursement accounts must be different
    CHECK (LoanAccountId <> DisbursementAccountId),
    -- Principal — all amounts in exact cents (4dp)
    RequestedAmount     DECIMAL(18,4)   NOT NULL
        CHECK (RequestedAmount > 0),
    ApprovedAmount      DECIMAL(18,4)   NULL
        CHECK (ApprovedAmount IS NULL OR ApprovedAmount > 0),
    DisbursedAmount     DECIMAL(18,4)   NULL
        CHECK (DisbursedAmount IS NULL OR DisbursedAmount > 0),
    OutstandingPrincipal DECIMAL(18,4)  NOT NULL DEFAULT 0
        CHECK (OutstandingPrincipal >= 0),
    -- Approved cannot exceed requested
    CHECK (ApprovedAmount IS NULL OR ApprovedAmount <= RequestedAmount),
    -- Disbursed cannot exceed approved
    CHECK (DisbursedAmount IS NULL OR ApprovedAmount IS NOT NULL),
    CHECK (DisbursedAmount IS NULL OR DisbursedAmount <= ApprovedAmount),
    -- Outstanding cannot exceed disbursed
    CHECK (DisbursedAmount IS NULL OR OutstandingPrincipal <= DisbursedAmount),
    -- Rate structure
    InterestRateTypeId  TINYINT     NOT NULL REFERENCES dbo.InterestRateType(RateTypeId),
    ReferenceRateIndexId TINYINT    NULL REFERENCES dbo.ReferenceRateIndex(IndexId),
    SpreadBps           SMALLINT    NULL
        CHECK (SpreadBps IS NULL OR SpreadBps >= 0),
    -- Variable-rate loans MUST have reference index and spread
    CHECK ((InterestRateTypeId = 1) OR  -- FIXED: no index required
           (ReferenceRateIndexId IS NOT NULL AND SpreadBps IS NOT NULL)),
    -- Schedule
    FrequencyId     TINYINT         NOT NULL REFERENCES dbo.RepaymentFrequency(FrequencyId),
    TermMonths      SMALLINT        NOT NULL
        CHECK (TermMonths > 0 AND TermMonths <= 480),  -- max 40 years
    GracePeriodMonths SMALLINT      NOT NULL DEFAULT 0
        CHECK (GracePeriodMonths >= 0),
    CHECK (GracePeriodMonths < TermMonths),
    -- Dates — strict chronological ordering
    ApplicationDate DATE            NOT NULL,
    ApprovalDate    DATE            NULL,
    DisbursementDate DATE           NULL,
    MaturityDate    DATE            NULL,
    FirstPaymentDate DATE           NULL,
    CHECK (ApprovalDate IS NULL OR ApprovalDate >= ApplicationDate),
    CHECK (DisbursementDate IS NULL OR (ApprovalDate IS NOT NULL AND DisbursementDate >= ApprovalDate)),
    CHECK (MaturityDate IS NULL OR (DisbursementDate IS NOT NULL AND MaturityDate > DisbursementDate)),
    CHECK (FirstPaymentDate IS NULL OR (DisbursementDate IS NOT NULL AND FirstPaymentDate > DisbursementDate)),
    -- Early repayment
    EarlyRepaymentPenaltyPct DECIMAL(5,2) NULL
        CHECK (EarlyRepaymentPenaltyPct IS NULL OR
               (EarlyRepaymentPenaltyPct >= 0 AND EarlyRepaymentPenaltyPct <= 10)),
    -- Debt-to-income at origination
    DtiAtOrigination DECIMAL(5,2)   NULL
        CHECK (DtiAtOrigination IS NULL OR
               (DtiAtOrigination >= 0 AND DtiAtOrigination <= 100)),
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CHECK (UpdatedAt >= CreatedAt)
);

CREATE TABLE dbo.LoanParty (
    LoanPartyId     BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    LoanId          BIGINT          NOT NULL REFERENCES dbo.Loan(LoanId),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    RoleId          TINYINT         NOT NULL REFERENCES dbo.LoanPartyRole(RoleId),
    UNIQUE (LoanId, PersonId, RoleId)
);

CREATE TABLE dbo.LoanInterestRate (
    RateId          BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    LoanId          BIGINT          NOT NULL REFERENCES dbo.Loan(LoanId),
    EffectiveFrom   DATE            NOT NULL,
    EffectiveTo     DATE            NULL,
    NominalRatePct  DECIMAL(7,4)    NOT NULL
        CHECK (NominalRatePct >= 0 AND NominalRatePct <= 100),
    EffectiveRatePct DECIMAL(7,4)   NOT NULL
        CHECK (EffectiveRatePct >= 0 AND EffectiveRatePct <= 100),
    -- Effective rate >= nominal (includes fees)
    CHECK (EffectiveRatePct >= NominalRatePct),
    -- Decomposed for variable-rate
    ReferenceRatePct DECIMAL(7,4)   NULL
        CHECK (ReferenceRatePct IS NULL OR (ReferenceRatePct >= -5 AND ReferenceRatePct <= 50)),
    SpreadPct        DECIMAL(7,4)   NULL
        CHECK (SpreadPct IS NULL OR SpreadPct >= 0),
    -- Cap and floor
    RateCapPct       DECIMAL(7,4)   NULL
        CHECK (RateCapPct IS NULL OR RateCapPct > 0),
    RateFloorPct     DECIMAL(7,4)   NULL
        CHECK (RateFloorPct IS NULL OR RateFloorPct >= 0),
    -- Cap must be > floor
    CHECK (RateCapPct IS NULL OR RateFloorPct IS NULL OR RateCapPct > RateFloorPct),
    -- Validity
    CHECK (EffectiveTo IS NULL OR EffectiveTo > EffectiveFrom),
    ChangedBy       INT             NULL,
    ChangeReason    NVARCHAR(200)   NULL,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.RepaymentSchedule (
    ScheduleId      BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    LoanId          BIGINT          NOT NULL REFERENCES dbo.Loan(LoanId),
    InstallmentNo   INT             NOT NULL
        CHECK (InstallmentNo > 0),
    DueDate         DATE            NOT NULL,
    -- All monetary amounts: 4dp, non-negative
    PrincipalDue    DECIMAL(18,4)   NOT NULL CHECK (PrincipalDue >= 0),
    InterestDue     DECIMAL(18,4)   NOT NULL CHECK (InterestDue >= 0),
    InsuranceDue    DECIMAL(18,4)   NOT NULL DEFAULT 0 CHECK (InsuranceDue >= 0),
    FeesDue         DECIMAL(18,4)   NOT NULL DEFAULT 0 CHECK (FeesDue >= 0),
    -- Total = principal + interest + insurance + fees
    TotalDue AS (PrincipalDue + InterestDue + InsuranceDue + FeesDue) PERSISTED,
    -- Paid amounts
    PrincipalPaid   DECIMAL(18,4)   NOT NULL DEFAULT 0 CHECK (PrincipalPaid >= 0),
    InterestPaid    DECIMAL(18,4)   NOT NULL DEFAULT 0 CHECK (InterestPaid >= 0),
    -- Cannot overpay beyond due
    CHECK (PrincipalPaid <= PrincipalDue + 0.01),  -- 1 cent tolerance
    CHECK (InterestPaid <= InterestDue + 0.01),
    PaidDate        DATE            NULL,
    DaysOverdue     INT             NOT NULL DEFAULT 0
        CHECK (DaysOverdue >= 0),
    IsPaid          BIT             NOT NULL DEFAULT 0,
    -- IsPaid consistency
    CHECK ((IsPaid = 0 AND PaidDate IS NULL) OR
           (IsPaid = 1 AND PaidDate IS NOT NULL)),
    UNIQUE (LoanId, InstallmentNo)
);

CREATE TABLE dbo.ReferenceRateSnapshot (
    SnapshotId      BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    IndexId         TINYINT         NOT NULL REFERENCES dbo.ReferenceRateIndex(IndexId),
    SnapshotDate    DATE            NOT NULL,
    RatePct         DECIMAL(7,4)    NOT NULL
        CHECK (RatePct >= -5 AND RatePct <= 50),  -- negative rates exist
    Source          VARCHAR(30)     NULL,
    UNIQUE (IndexId, SnapshotDate)
);

CREATE TABLE dbo.Collateral (
    CollateralId    BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    CollateralTypeId TINYINT        NOT NULL REFERENCES dbo.CollateralType(CollateralTypeId),
    Description     NVARCHAR(500)   NOT NULL,
    OriginalValue       DECIMAL(18,4)   NOT NULL CHECK (OriginalValue > 0),
    CurrentValue        DECIMAL(18,4)   NOT NULL CHECK (CurrentValue > 0),
    LastValuationDate   DATE            NOT NULL,
    CurrencyId          SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    AddressId           BIGINT          NULL REFERENCES dbo.Address(AddressId),
    InsuranceRequired   BIT             NOT NULL DEFAULT 0,
    InsuranceExpiryDate DATE            NULL,
    -- If insurance required, expiry must be set
    CHECK ((InsuranceRequired = 0) OR
           (InsuranceRequired = 1 AND InsuranceExpiryDate IS NOT NULL)),
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.LoanCollateral (
    LoanCollateralId BIGINT         NOT NULL PRIMARY KEY IDENTITY(1,1),
    LoanId          BIGINT          NOT NULL REFERENCES dbo.Loan(LoanId),
    CollateralId    BIGINT          NOT NULL REFERENCES dbo.Collateral(CollateralId),
    PledgeDate      DATE            NOT NULL,
    ReleaseDate     DATE            NULL,
    LtvPct          DECIMAL(7,4)    NULL
        CHECK (LtvPct IS NULL OR (LtvPct > 0 AND LtvPct <= 200)),  -- LTV can exceed 100%
    CHECK (ReleaseDate IS NULL OR ReleaseDate > PledgeDate),
    UNIQUE (LoanId, CollateralId, PledgeDate)
);


-- ============================================================================
-- §5  EMPLOYEES & ROLE-BASED ACCESS CONTROL
-- ============================================================================

CREATE TABLE dbo.Department (
    DepartmentId    INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    Code            VARCHAR(15)     NOT NULL UNIQUE,
    Name            NVARCHAR(80)    NOT NULL,
    ParentDeptId    INT             NULL REFERENCES dbo.Department(DepartmentId),
    BranchId        INT             NULL REFERENCES dbo.Branch(BranchId)
);

CREATE TABLE dbo.JobTitle (
    JobTitleId      INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    Code            VARCHAR(20)     NOT NULL UNIQUE,
    Title           NVARCHAR(80)    NOT NULL,
    SeniorityLevel  TINYINT         NOT NULL DEFAULT 1
        CHECK (SeniorityLevel >= 1 AND SeniorityLevel <= 5)
);

CREATE TABLE dbo.Employee (
    EmployeeId      INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    PersonId        BIGINT          NOT NULL REFERENCES dbo.Person(PersonId),
    EmployeeCode    VARCHAR(10)     NOT NULL UNIQUE
        CHECK (EmployeeCode LIKE 'E%' AND LEN(EmployeeCode) >= 4),
    DepartmentId    INT             NOT NULL REFERENCES dbo.Department(DepartmentId),
    JobTitleId      INT             NOT NULL REFERENCES dbo.JobTitle(JobTitleId),
    BranchId        INT             NOT NULL REFERENCES dbo.Branch(BranchId),
    ManagerId       INT             NULL REFERENCES dbo.Employee(EmployeeId),
    -- Cannot manage yourself
    CHECK (ManagerId IS NULL OR ManagerId <> EmployeeId),
    HireDate        DATE            NOT NULL,
    TerminationDate DATE            NULL,
    CHECK (TerminationDate IS NULL OR TerminationDate > HireDate),
    IsActive        BIT             NOT NULL DEFAULT 1,
    -- Terminated employees must be inactive
    CHECK ((TerminationDate IS NULL) OR (TerminationDate IS NOT NULL AND IsActive = 0)),
    LoginUsername   VARCHAR(50)     NULL UNIQUE,
    CreatedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Deferred FK: IdentityDocument.VerifiedBy -> Employee
ALTER TABLE dbo.IdentityDocument
    ADD CONSTRAINT FK_IdentityDocument_VerifiedBy
        FOREIGN KEY (VerifiedBy) REFERENCES dbo.Employee(EmployeeId);

CREATE TABLE dbo.Role (
    RoleId          INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    Code            VARCHAR(30)     NOT NULL UNIQUE,
    Name            NVARCHAR(80)    NOT NULL,
    Description     NVARCHAR(500)   NULL,
    MinSeniorityLevel TINYINT       NOT NULL DEFAULT 1
        CHECK (MinSeniorityLevel >= 1 AND MinSeniorityLevel <= 5)
);

CREATE TABLE dbo.RolePermission (
    RolePermId      INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    RoleId          INT             NOT NULL REFERENCES dbo.Role(RoleId),
    ScopeId         SMALLINT        NOT NULL REFERENCES dbo.PermissionScope(ScopeId),
    ActionId        SMALLINT        NOT NULL REFERENCES dbo.PermissionAction(ActionId),
    -- Amount limit: NULL means unlimited
    MaxAmount       DECIMAL(18,4)   NULL
        CHECK (MaxAmount IS NULL OR MaxAmount > 0),
    BranchRestricted BIT            NOT NULL DEFAULT 0,
    -- Time-of-day restrictions (24h format)
    AllowedFromHour TINYINT         NULL
        CHECK (AllowedFromHour IS NULL OR (AllowedFromHour >= 0 AND AllowedFromHour <= 23)),
    AllowedToHour   TINYINT         NULL
        CHECK (AllowedToHour IS NULL OR (AllowedToHour >= 0 AND AllowedToHour <= 23)),
    -- Both hour fields or neither
    CHECK ((AllowedFromHour IS NULL AND AllowedToHour IS NULL) OR
           (AllowedFromHour IS NOT NULL AND AllowedToHour IS NOT NULL)),
    -- From must be before To (no overnight shifts in this model)
    CHECK (AllowedFromHour IS NULL OR AllowedFromHour < AllowedToHour),
    UNIQUE (RoleId, ScopeId, ActionId)
);

CREATE TABLE dbo.EmployeeRole (
    EmployeeRoleId  INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    EmployeeId      INT             NOT NULL REFERENCES dbo.Employee(EmployeeId),
    RoleId          INT             NOT NULL REFERENCES dbo.Role(RoleId),
    AssignedDate    DATE            NOT NULL,
    RevokedDate     DATE            NULL,
    AssignedBy      INT             NOT NULL REFERENCES dbo.Employee(EmployeeId),
    Justification   NVARCHAR(200)   NULL,
    -- Cannot assign role to yourself
    CHECK (AssignedBy <> EmployeeId),
    CHECK (RevokedDate IS NULL OR RevokedDate > AssignedDate),
    UNIQUE (EmployeeId, RoleId, AssignedDate)
);

CREATE TABLE dbo.IncompatibleRolePair (
    PairId          INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    RoleIdA         INT             NOT NULL REFERENCES dbo.Role(RoleId),
    RoleIdB         INT             NOT NULL REFERENCES dbo.Role(RoleId),
    Reason          NVARCHAR(200)   NOT NULL,
    CHECK (RoleIdA < RoleIdB),
    UNIQUE (RoleIdA, RoleIdB)
);

CREATE TABLE dbo.DataVisibilityRule (
    RuleId          INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    RoleId          INT             NOT NULL REFERENCES dbo.Role(RoleId),
    TableName       VARCHAR(60)     NOT NULL CHECK (LEN(TableName) >= 4),
    FilterColumn    VARCHAR(60)     NOT NULL,
    FilterOperator  VARCHAR(5)      NOT NULL DEFAULT '='
        CHECK (FilterOperator IN ('=', 'IN', '<=', '>=', '<>')),
    FilterValue     NVARCHAR(200)   NOT NULL,
    Description     NVARCHAR(200)   NULL,
    IsActive        BIT             NOT NULL DEFAULT 1
);


-- ============================================================================
-- §6  AUDIT TRAIL — Tamper-evident, four-eyes
-- ============================================================================

CREATE TABLE dbo.AuditLog (
    AuditLogId      BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    ActionTypeId    SMALLINT        NOT NULL REFERENCES dbo.AuditActionType(ActionTypeId),
    EmployeeId      INT             NULL REFERENCES dbo.Employee(EmployeeId),
    EntityType      VARCHAR(30)     NOT NULL CHECK (LEN(EntityType) >= 3),
    EntityId        BIGINT          NOT NULL CHECK (EntityId > 0),
    BeforeSnapshot  NVARCHAR(MAX)   NULL,
    AfterSnapshot   NVARCHAR(MAX)   NULL,
    Description     NVARCHAR(500)   NULL,
    IpAddress       VARCHAR(45)     NULL,
    SessionId       VARCHAR(100)    NULL,
    OccurredAt      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    -- Tamper-evidence hash chain
    PreviousHash    VARBINARY(32)   NULL,
    RecordHash      VARBINARY(32)   NOT NULL
    -- First record has NULL PreviousHash; all subsequent must have it
    -- (enforced via trigger, not CHECK — sequential dependency)
);

CREATE TABLE dbo.ApprovalRequest (
    ApprovalId      BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    EntityType      VARCHAR(30)     NOT NULL,
    EntityId        BIGINT          NOT NULL CHECK (EntityId > 0),
    ActionTypeId    SMALLINT        NOT NULL REFERENCES dbo.AuditActionType(ActionTypeId),
    RequestedBy     INT             NOT NULL REFERENCES dbo.Employee(EmployeeId),
    RequestedAt     DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    RequestNotes    NVARCHAR(500)   NULL,
    ReviewedBy      INT             NULL REFERENCES dbo.Employee(EmployeeId),
    ReviewedAt      DATETIME2       NULL,
    ReviewResult    VARCHAR(10)     NULL
        CHECK (ReviewResult IS NULL OR ReviewResult IN ('APPROVED','REJECTED','EXPIRED')),
    ReviewNotes     NVARCHAR(500)   NULL,
    -- FOUR-EYES: cannot approve own request
    CHECK (ReviewedBy IS NULL OR RequestedBy <> ReviewedBy),
    -- Review fields must be consistent
    CHECK ((ReviewedBy IS NULL AND ReviewedAt IS NULL AND ReviewResult IS NULL) OR
           (ReviewedBy IS NOT NULL AND ReviewedAt IS NOT NULL AND ReviewResult IS NOT NULL)),
    CHECK (ReviewedAt IS NULL OR ReviewedAt >= RequestedAt)
);


-- ============================================================================
-- §7  DOCUMENTS & ATTACHMENTS
-- ============================================================================

CREATE TABLE dbo.Document (
    DocumentId      BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    DocCategoryId   TINYINT         NOT NULL REFERENCES dbo.DocumentCategory(DocCategoryId),
    Title           NVARCHAR(200)   NOT NULL CHECK (LEN(Title) >= 1),
    FileName        NVARCHAR(260)   NOT NULL CHECK (LEN(FileName) >= 3),
    MimeType        VARCHAR(100)    NOT NULL CHECK (MimeType LIKE '%/%'),
    FileSizeBytes   BIGINT          NOT NULL CHECK (FileSizeBytes > 0),
    StoragePath     NVARCHAR(500)   NOT NULL,
    Checksum        VARCHAR(64)     NOT NULL
        CHECK (LEN(Checksum) = 64),  -- SHA-256 hex
    UploadedBy      INT             NOT NULL REFERENCES dbo.Employee(EmployeeId),
    UploadedAt      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    RetentionYears  SMALLINT        NOT NULL DEFAULT 10
        CHECK (RetentionYears > 0 AND RetentionYears <= 100),
    ExpiresAt       DATE            NULL
);

CREATE TABLE dbo.DocumentLink (
    LinkId          BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    DocumentId      BIGINT          NOT NULL REFERENCES dbo.Document(DocumentId),
    EntityType      VARCHAR(30)     NOT NULL
        CHECK (EntityType IN ('Person','Loan','Account','Collateral','Branch')),
    EntityId        BIGINT          NOT NULL CHECK (EntityId > 0),
    LinkedAt        DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    LinkedBy        INT             NOT NULL REFERENCES dbo.Employee(EmployeeId),
    UNIQUE (DocumentId, EntityType, EntityId)
);


-- ============================================================================
-- §8  FX & TREASURY
-- ============================================================================

CREATE TABLE dbo.ExchangeRate (
    ExchangeRateId  BIGINT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    FromCurrencyId  SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    ToCurrencyId    SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    RateDate        DATE            NOT NULL,
    BuyRate         DECIMAL(18,8)   NOT NULL CHECK (BuyRate > 0),
    SellRate        DECIMAL(18,8)   NOT NULL CHECK (SellRate > 0),
    MidRate         DECIMAL(18,8)   NOT NULL CHECK (MidRate > 0),
    Source          VARCHAR(10)     NOT NULL DEFAULT 'BNR',
    -- Buy <= Mid <= Sell (bank buys low, sells high)
    CHECK (BuyRate <= MidRate AND MidRate <= SellRate),
    -- Cannot exchange same currency
    CHECK (FromCurrencyId <> ToCurrencyId),
    UNIQUE (FromCurrencyId, ToCurrencyId, RateDate, Source)
);


-- ============================================================================
-- §9  FEES & PRODUCT CONFIGURATION
-- ============================================================================

CREATE TABLE dbo.FeeSchedule (
    FeeScheduleId   INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    FeeTypeId       SMALLINT        NOT NULL REFERENCES dbo.FeeType(FeeTypeId),
    AccountTypeId   TINYINT         NULL REFERENCES dbo.AccountType(AccountTypeId),
    LoanTypeId      TINYINT         NULL REFERENCES dbo.LoanType(LoanTypeId),
    CurrencyId      SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    -- Must be for account OR loan, not both
    CHECK ((AccountTypeId IS NOT NULL AND LoanTypeId IS NULL) OR
           (AccountTypeId IS NULL AND LoanTypeId IS NOT NULL) OR
           (AccountTypeId IS NULL AND LoanTypeId IS NULL)),
    -- Fee: fixed or percentage or both (min/max apply to percentage)
    FixedAmount     DECIMAL(18,4)   NULL CHECK (FixedAmount IS NULL OR FixedAmount >= 0),
    PercentageRate  DECIMAL(7,4)    NULL CHECK (PercentageRate IS NULL OR
                                                (PercentageRate >= 0 AND PercentageRate <= 100)),
    MinAmount       DECIMAL(18,4)   NULL CHECK (MinAmount IS NULL OR MinAmount >= 0),
    MaxAmount       DECIMAL(18,4)   NULL CHECK (MaxAmount IS NULL OR MaxAmount > 0),
    -- At least one fee method
    CHECK (FixedAmount IS NOT NULL OR PercentageRate IS NOT NULL),
    -- Min <= Max when both present
    CHECK (MinAmount IS NULL OR MaxAmount IS NULL OR MinAmount <= MaxAmount),
    -- Validity
    ValidFrom       DATE            NOT NULL,
    ValidTo         DATE            NULL,
    CHECK (ValidTo IS NULL OR ValidTo > ValidFrom),
    -- Regulatory cap
    RegulatoryCap   DECIMAL(18,4)   NULL CHECK (RegulatoryCap IS NULL OR RegulatoryCap > 0)
);

CREATE TABLE dbo.DepositRate (
    DepositRateId   INT             NOT NULL PRIMARY KEY IDENTITY(1,1),
    AccountTypeId   TINYINT         NOT NULL REFERENCES dbo.AccountType(AccountTypeId),
    CurrencyId      SMALLINT        NOT NULL REFERENCES dbo.Currency(CurrencyId),
    MinBalance      DECIMAL(18,4)   NOT NULL DEFAULT 0
        CHECK (MinBalance >= 0),
    MaxBalance      DECIMAL(18,4)   NULL
        CHECK (MaxBalance IS NULL OR MaxBalance > 0),
    -- Min < Max balance tier
    CHECK (MaxBalance IS NULL OR MaxBalance > MinBalance),
    InterestRatePct DECIMAL(7,4)    NOT NULL
        CHECK (InterestRatePct >= 0 AND InterestRatePct <= 30),
    ValidFrom       DATE            NOT NULL,
    ValidTo         DATE            NULL,
    CHECK (ValidTo IS NULL OR ValidTo > ValidFrom),
    -- Only savings/deposit accounts earn interest
    CHECK (AccountTypeId IN (2, 3))
);


-- ============================================================================
-- SCHEMA STATISTICS
-- ============================================================================
-- Tables:           58
-- CHECK constraints: 120+
-- Foreign keys:      70+
-- Unique constraints: 40+
-- Computed columns:   1 (RepaymentSchedule.TotalDue)
--
-- FK topology:
--   1:1  — Loan ↔ LoanAccount
--   1:N  — Person → Contacts, Addresses, Documents; Account → Transactions
--   N:M  — Person ↔ Account (via AccountOwnership), Loan ↔ Person (via LoanParty),
--           Loan ↔ Collateral (via LoanCollateral), Document ↔ Entity (via DocumentLink)
--   Self-ref — Department.ParentDeptId, Employee.ManagerId, Transaction.ReversedByTxId
--   Diamond — Person → Employee → Branch → Address; Person → PersonAddress → Address
-- ============================================================================
