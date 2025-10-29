MATCH(n) DETACH DELETE n;

CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.accountId);
CREATE INDEX IF NOT EXISTS FOR (l:Loan) ON (l.loanId);
CREATE INDEX IF NOT EXISTS FOR (c:Company) ON (c.companyId);
CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.personId);
CREATE INDEX IF NOT EXISTS FOR (m:Medium) ON (m.mediumId);


CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/Account.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "CREATE (:Account {
    accountId: toInteger(row.accountId),
    createTime: datetime(replace(row.createTime, ' ', 'T')),
    isBlocked: toBoolean(row.isBlocked),
    accountType: toString(row.accountType),
    nickname: toString(row.nickname),
    phonenum: toString(row.phonenum),
    email: toString(row.email),
    freqLoginType: toString(row.freqLoginType),
    lastLoginTime: datetime({epochMillis: toInteger(row.lastLoginTime)}),
    accountLevel: toString(row.accountLevel)
  })",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/Company.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "CREATE (:Company {
    companyId: toInteger(row.companyId),
    companyName: toString(row.companyName),
    isBlocked: toBoolean(row.isBlocked),
    createTime: datetime(replace(row.createTime, ' ', 'T')),
    country: toString(row.country),
    city: toString(row.city),
    business: toString(row.business),
    description: toString(row.description),
    url: toString(row.url)
  })",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/Loan.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "CREATE (:Loan {
    loanId: toInteger(row.loanId),
    loanAmount: toFloat(row.loanAmount),
    balance: toFloat(row.balance),
    createTime: datetime(replace(row.createTime, ' ', 'T')),
    loanUsage: toString(row.loanUsage),
    interestRate: toFloat(row.interestRate),
    flow_rate: 'TS_flow_rate'
  })",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/Medium.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "CREATE (:Medium {
    mediumId: toInteger(row.mediumId),
    mediumType: toString(row.mediumType),
    isBlocked: toBoolean(row.isBlocked),
    createTime: datetime(replace(row.createTime, ' ', 'T')),
    lastLoginTime: datetime({epochMillis: toInteger(row.lastLoginTime)}),
    riskLevel: toString(row.riskLevel)
  })",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/Person.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "CREATE (:Person {
    personId: toInteger(row.personId),
    personName: toString(row.personName),
    isBlocked: toBoolean(row.isBlocked),
    createTime: datetime(replace(row.createTime, ' ', 'T')),
    gender: toString(row.gender),
    birthday: datetime(replace(row.birthday, ' ', 'T')),
    country: toString(row.country),
    city: toString(row.city)
  })",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/AccountRepayLoan.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Account {accountId: toInteger(row.accountId)}), (l:Loan {loanId: toInteger(row.loanId)})
   CREATE (a)-[:Repay {amount: toFloat(row.amount), createTime: datetime(replace(row.createTime, ' ', 'T'))}]->(l)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/LoanDepositAccount.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Account {accountId: toInteger(row.accountId)}), (l:Loan {loanId: toInteger(row.loanId)})
   CREATE (l)-[:Deposit {amount: toFloat(row.amount), createTime: datetime(replace(row.createTime, ' ', 'T'))}]->(a)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/AccountTransferAccount.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Account {accountId: toInteger(row.fromId)}), (b:Account {accountId: toInteger(row.toId)})
   CREATE (a)-[:Transfer {
     amount: toFloat(row.amount),
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     orderNum: toInteger(row.orderNum),
     comment: toString(row.comment),
     payType: toString(row.payType),
     goodsType: toString(row.goodsType)
   }]->(b)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/AccountWithdrawAccount.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Account {accountId: toInteger(row.fromId)}), (b:Account {accountId: toInteger(row.toId)})
   CREATE (a)-[:Withdraw {
     amount: toFloat(row.amount),
     createTime: datetime(replace(row.createTime, ' ', 'T'))
   }]->(b)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/CompanyApplyLoan.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (c:Company {companyId: toInteger(row.companyId)}), (l:Loan {loanId: toInteger(row.loanId)})
   CREATE (c)-[:Apply {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     org: toString(row.org)
   }]->(l)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/CompanyGuaranteeCompany.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Company {companyId: toInteger(row.fromId)}), (b:Company {companyId: toInteger(row.toId)})
   CREATE (a)-[:Guarantee {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     relation: toString(row.relation)
   }]->(b)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/CompanyInvestCompany.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Company {companyId: toInteger(row.investorId)}), (b:Company {companyId: toInteger(row.companyId)})
   CREATE (a)-[:Invest {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     ratio: toFloat(row.ratio)
   }]->(b)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/CompanyOwnAccount.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (c:Company {companyId: toInteger(row.companyId)}), (a:Account {accountId: toInteger(row.accountId)})
   CREATE (c)-[:Own {
     createTime: datetime(replace(row.createTime, ' ', 'T'))
   }]->(a)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/MediumSignInAccount.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (m:Medium {mediumId: toInteger(row.mediumId)}), (a:Account {accountId: toInteger(row.accountId)})
   CREATE (m)-[:SignIn {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     location: toString(row.location)
   }]->(a)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/PersonApplyLoan.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (p:Person {personId: toInteger(row.personId)}), (l:Loan {loanId: toInteger(row.loanId)})
   CREATE (p)-[:Apply {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     org: toString(row.org)
   }]->(l)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/PersonGuaranteePerson.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Person {personId: toInteger(row.fromId)}), (b:Person {personId: toInteger(row.toId)})
   CREATE (a)-[:Guarantee {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     relation: toString(row.relation)
   }]->(b)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/PersonInvestCompany.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (a:Person {personId: toInteger(row.investorId)}), (b:Company {companyId: toInteger(row.companyId)})
   CREATE (a)-[:Invest {
     createTime: datetime(replace(row.createTime, ' ', 'T')),
     ratio: toFloat(row.ratio)
   }]->(b)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///sf0.01/snapshot/PersonOwnAccount.csv' AS row FIELDTERMINATOR '|' RETURN row",
  "MATCH (p:Person {personId: toInteger(row.personId)}), (a:Account {accountId: toInteger(row.accountId)})
   CREATE (p)-[:Own {
     createTime: datetime(replace(row.createTime, ' ', 'T'))
   }]->(a)",
  {batchSize: 3000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:////sf0.01/snapshot/account_balances.csv' AS row RETURN row",
  "
  MATCH (n:Account {accountId: toInteger(row.id)})
  SET n.balance_timestamps = [ts IN apoc.convert.fromJsonList(row.balance_timestamps) | datetime(ts)],
      n.balance_values = [val IN apoc.convert.fromJsonList(row.balance_values) | toFloat(val)]
  ",
  {batchSize: 3000, parallel: false}
);

CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.balance_timestamps, a.balance_values);


