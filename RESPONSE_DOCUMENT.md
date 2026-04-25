# API Request / Response Documentation

## 1.

### 1.1  
**Success Request/Response**

**Correct parameter values**

**Request API:**  
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Company_master&date=ddmmyyyy&section=Fundamental&sub=&token={token}

**Response:**  
Status code 200 (Company master incremental data will be displayed)

**Note:**  
For specific file if there is no any recent updation then incremental data will not be displayed even after passing correct parameter values. In this case also you will receive Status code 204 No Content.

---

## 2.

### 2.1  
**Failure Request/Response**

**Incorrect parameter value**

**Request API:**  
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=*Company_mater*&date=ddmmyyyy&section=Fundamental&sub=&token={token}

**Response:**  
Status code 204

Similar for other parameters if incorrect value is passed.

---

### 2.2  
**Incorrect URL**

**Request API:**  
https://contentapi.accordwebservices.com/*Raw*/GetRawDataJSON?filename=Company_master&date=ddmmyyyy&section=Fundamental&sub=&token={token}

**Response:**  
Status code 404

---

### 2.3  
**API requests from Non-whitelisted IP**

**Request API:**  
https://contentapi.accordwebservices.com/RawData/GetRawDataJSON?filename=Company_master&date=ddmmyyyy&section=Fundamental&sub=&token={token}

**Response:**  
Status code 403