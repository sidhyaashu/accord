# 📘 Accord Fundamental API

---

# 🧱 1. MASTER FILES

---

## 🔹 Company Master

**Description**
Company master is the main master for the company which include basic details of company listed in BSE / NSE exchange. This table gives you all the mappings to the other tables.

**File Name**

```text
Company_master
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Company_master&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Industry Master

**Description**
This table contains list of Industries. As this file contains master data, it will not be updated frequently, due to which it will not be available in feed files on daily basis. In case of any changes or new records it will be available for download.

**File Name**

```text
Industrymaster_Ex1
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Industrymaster_Ex1&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 House Master

**Description**
This table contains list of Business groups or business houses. As this file contains master data, it will not be updated frequently, due to which it will not be available in feed files on daily basis. In case of any changes or new records it will be available for download.

**File Name**

```text
Housemaster
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Housemaster&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Stock Exchange Master

**Description**
This table contains list of Stock Exchanges. As this file contains master data, it will not be updated frequently, due to which it will not be available in feed files on daily basis. In case of any changes or new records it will be available for download.

**File Name**

```text
Stockexchangemaster
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Stockexchangemaster&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Company Listings

**Description**
This data gives the list in which a company is listed. One company can have more than one stock exchange listings.

**File Name**

```text
Complistings
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Complistings&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Company Address

**Description**
This table contains Company registered office address details.

**File Name**

```text
Companyaddress
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Companyaddress&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

# 🏢 2. REGISTRAR

---

## 🔹 Registrar Master

**Description**
This data feed gives the details registrar such as name, address and contact information. As this file contains master data, it will not be updated frequently, due to which it will not be available in feed files on daily basis. In case of any changes or new records it will be available for download.

**File Name**

```text
Registrarmaster
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Registrarmaster&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Registrar Data

**Description**
This data feed will give the company’s registrar information. One Company can have multiple registrars.

**File Name**

```text
Registrardata
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Registrardata&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

# 👥 3. BOARD

---

## 🔹 Board of Directors

**Description**
Board of directors information of the company can be availed from this data feed.

**File Name**

```text
Board
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Board&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

# 📊 4. FINANCIALS

---

## 🔹 Balance Sheet Data

**Description**
This data feed provides the balance-sheet data of the Standalone | Consolidated company.

**File Names**

```text
Finance_bs
Finance_cons_bs
```

**URLs**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_bs&date=ddMMyyyy&section=Fundamental&sub=&token={token}
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_cons_bs&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Profit & Loss Data

**Description**
This data feed provides the profit & Loss data of the company.

**File Names**

```text
Finance_pl
Finance_cons_pl
```

**URLs**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_pl&date=ddMMyyyy&section=Fundamental&sub=&token={token}
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_cons_pl&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Cash Flow Data

**Description**
Cash flow data of the company is available in this data feed. Cash flow will be shown as per the display format.

**File Names**

```text
Finance_cf
Finance_cons_cf
```

**URLs**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_cf&date=ddMMyyyy&section=Fundamental&sub=&token={token}
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_cons_cf&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Financial Ratios

**Description**
Company’s financial ratios are available in this data feed.

**File Names**

```text
Finance_fr
Finance_cons_fr
```

**URLs**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_fr&date=ddMMyyyy&section=Fundamental&sub=&token={token}
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Finance_cons_fr&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

# 📈 5. RESULTS

---

## 🔹 Results IND-AS

**Description**
This data feed provides the Results IND-AS format data. *(From section 5.2)*

**File Names**

```text
Resultsf_IND_Ex1
Resultsf_IND_Cons_Ex1
```

**URLs**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Resultsf_IND_Ex1&date=ddMMyyyy&section=Fundamental&sub=&token={token}
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Resultsf_IND_Cons_Ex1&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

# 🏢 6. COMPANY EQUITY

---

## 🔹 Company Equity

**Description**
Company Equity provides the latest Mcap, Face value, PE etc. of the company. 

**File Names**

```text
company_equity
company_equity_cons
```

**URLs**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=company_equity&date=ddMMyyyy&section=CompanyEquity&sub=&token={token}
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=company_equity_cons&date=ddMMyyyy&section=CompanyEquity&sub=&token={token}
```

---

# 📊 7. SHAREHOLDING

---

## 🔹 Shareholding Pattern

**Description**
This data feed gives the shareholding pattern details of the company.

**File Name**

```text
Shpsummary
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Shpsummary&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Shareholders Name

**Description**
This data feed provides the shareholders details including name and shareholding percentage.

**File Name**

```text
Shp_details
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Shp_details&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Shareholders Category Master

**Description**
This table gives shareholders category details. 

**File Name**

```text
Shp_catmaster_2
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Shp_catmaster_2&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

# 📉 8. MONTHLY SHARE PRICE

---

## 🔹 Monthly Share Price BSE

**Description**
Monthly share price data for BSE-listed companies.

**File Name**

```text
Monthlyprice
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Monthlyprice&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```

---

## 🔹 Monthly Share Price NSE

**Description**
Monthly share price data for NSE-listed companies.

**File Name**

```text
Nse_Monthprice
```

**URL**

```text
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Nse_Monthprice&date=ddMMyyyy&section=Fundamental&sub=&token={token}
```