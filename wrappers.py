__author__ = 'pgrimberg'
import web
from web import form
import blpapi
from data_structures import apiMsg
from optparse import OptionParser
SERVER_HOST = '10.16.1.19'
SERVER_PORT = 8194
SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")
BACK_BUTTON = "<button onclick=\"goBack()\">Back</button> <script> function goBack() { window.history.back();}</script>"
UUID = 9339085
IP_ADD = '10.16.1.204'


class bwrapper:
    def __init__(self):
        self.last_error = ""

    def open_blpapi_session(self):
        # Create session options
        parser = OptionParser(description="Retrieve reference data.")
        # parser.add_option("-a", "--ip", dest="host", help="server name or IP (default: %default)", metavar="ipAddress", default="localhost")
        # parser.add_option("-p", dest="port", type="int", help="server port (default: %default)", metavar="tcpPort", default=8194)
        (options, args) = parser.parse_args()

        # Fill session with the options
        session_options = blpapi.SessionOptions()
        authOptions = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=WATER_ISLAND:Portal"
        session_options.setAuthenticationOptions(authOptions)

        # session_options.setServerHost(SERVER_HOST)
        # session_options.setServerPort(SERVER_PORT)

        # Create a Session
        session = blpapi.Session(session_options)
        if not session.start():
            print "Failed to start session."
            return

        session.openService("//blp/apiauth")
        authService = session.getService("//blp/apiauth")
        authRequest = authService.createAuthorizationRequest()
        authRequest.set("uuid",UUID)
        authRequest.set("ipAddress",IP_ADD)
        # Submit a token generation request
        #tokenReqId = blpapi.CorrelationId(99)
        # token = session.generateToken(tokenReqId)
        # print('Generated Token Is: ')
        # print(token)
        # Start tbe Session




        return session

    def open_service(self, session, svc):
        if session is None:
            return None
        if not session.openService(svc):
            session.stop()
            return None

        return session.getService(svc)

    def processMessage(self, msg):
        if not msg.hasElement(SECURITY_DATA):
            self.last_error = "Unexpected message: " + msg
            return None
        # append data to returned dictionary
        api_msg = apiMsg()
        securityDataArray = msg.getElement(SECURITY_DATA)
        for securityData in securityDataArray.values():
            sec_name = securityData.getElementAsString(SECURITY)
            api_msg.add_security(sec_name)
            for field in securityData.getElement(FIELD_DATA).elements():
                field_name = str(field.name())
                if not field.isValid:
                    api_msg.add_error(sec_name,field_name,"invalid field")
                    continue
                if field.isArray():
                    for i,row in enumerate(field.values()):
                        arrayFld = {}
                        for elem in row.elements():
                            arrayFld[str(elem.name())] = elem.getValueAsString() if not elem.isNull() else "NULL"
                            api_msg.add_field(sec_name, field_name, arrayFld)
                else:
                    api_msg.add_field(sec_name, field_name, field.getValueAsString())
        # handle errors
        for fieldException in securityData.getElement(FIELD_EXCEPTIONS).values():
            errorInfo = fieldException.getElement(ERROR_INFO)
            category = errorInfo.getElementAsString("category")
            err_value = fieldException.getElementAsString(FIELD_ID)
            api_msg.add_error(sec_name, category, err_value)

        return api_msg

    def get_last_error_message(self):
        res = self.last_error
        self.last_error = ""
        return res

    def get_refdata_fields(self, bbgids, fields, overrides_dict):
        session = self.open_blpapi_session()
        if session is None:
            self.last_error = "failed to open bloomberg session"
            return False
        # open service
        svc = "//blp/refdata"
        service = self.open_service(session, svc)
        if service is None:
            self.last_error = "failed to open bloomberg service " + svc
            return False
        bbg_request = service.createRequest("ReferenceDataRequest")
        # append securities to request
        for bbgid in bbgids:
            bbg_request.append("securities", "/bbgid/" + bbgid)
        # append fields to request
        for fld in fields:
            bbg_request.append("fields",fld)
        # add overrides
        overrides = bbg_request.getElement("overrides")
        for key in overrides_dict:
            fieldId = key
            value = overrides_dict[key]
            ovrd = overrides.appendElement()
            ovrd.setElement("fieldId", fieldId)
            ovrd.setElement("value", value)

        cid = session.sendRequest(bbg_request)

        api_msg_list = []
        try:
            # Process received events
            while True:
                # We provide timeout to give the chance to Ctrl+C handling:
                event = session.nextEvent(500)
                for msg in event:
                    if cid in msg.correlationIds():
                        api_msg = self.processMessage(msg)
                        api_msg_list.append(api_msg)

                # Response completely received, so we could exit
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            # Stop the session
            session.stop()
            return api_msg_list

    def get_refdata_fields2(self, sec_ids, sec_id_type, fields, overrides_dict):
        session = self.open_blpapi_session()
        if session is None:
            self.last_error = "failed to open bloomberg session"
            return False
        # open service
        svc = "//blp/refdata"
        service = self.open_service(session, svc)
        if service is None:
            self.last_error = "failed to open bloomberg service " + svc
            return False
        bbg_request = service.createRequest("ReferenceDataRequest")

        if sec_id_type == " ":
            for bbgid in sec_ids:
                bbg_request.append("securities", "/bbgid/" + bbgid)
        else: # assume non BBGID is ticker
            for ticker in sec_ids:
                bbg_request.append("securities", ticker)
        # append fields to request
        for fld in fields:
            bbg_request.append("fields",fld)

        # add overrides
        overrides = bbg_request.getElement("overrides")
        for key in overrides_dict:
            fieldId = key
            value = overrides_dict[key]
            ovrd = overrides.appendElement()
            ovrd.setElement("fieldId", fieldId)
            ovrd.setElement("value", value)

        cid = session.sendRequest(bbg_request)

        api_msg_list = []
        try:
            # Process received events
            while True:
                # We provide timeout to give the chance to Ctrl+C handling:
                event = session.nextEvent(500)
                for msg in event:
                    if cid in msg.correlationIds():
                        api_msg = self.processMessage(msg)
                        api_msg_list.append(api_msg)

                # Response completely received, so we could exit
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            # Stop the session
            session.stop()
            return api_msg_list

    def get_histdata_fields2(self, sec_ids, sec_id_type, fields, overrides_dict,start_date,end_date):
        session = self.open_blpapi_session()
        if session is None:
            self.last_error = "failed to open bloomberg session"
            return False
        # open service
        svc = "//blp/refdata"
        service = self.open_service(session, svc)
        if service is None:
            self.last_error = "failed to open bloomberg service " + svc
            return False
        bbg_request = service.createRequest("HistoricalDataRequest")
        # append securities to request
        if sec_id_type == "BBGID":
            for bbgid in sec_ids:
                bbg_request.append("securities", "/bbgid/" + bbgid)
        else: # assume non BBGID is ticker
            for ticker in sec_ids:
                bbg_request.append("securities", ticker)
        # append fields to request
        for fld in fields:
            bbg_request.append("fields",fld)

        # add dates
        bbg_request.set("startDate", start_date)
        bbg_request.set("endDate", end_date)

        # add overrides
        overrides = bbg_request.getElement("overrides")
        for key in overrides_dict:
            fieldId = key
            value = overrides_dict[key]
            ovrd = overrides.appendElement()
            ovrd.setElement("fieldId", fieldId)
            ovrd.setElement("value", value)

        cid = session.sendRequest(bbg_request)

        api_msg_list = []
        try:
            # Process received events
            while True:
                # We provide timeout to give the chance to Ctrl+C handling:
                event = session.nextEvent(500)
                for msg in event:
                    if cid in msg.correlationIds():
                        api_msg = self.processHistoricalDataResoponse(msg)
                        api_msg_list.append(api_msg)

                # Response completely received, so we could exit
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            # Stop the session
            session.stop()
            return api_msg_list

    def get_histdata_fields(self, sec_ids, sec_id_type, fields, overrides_dict,start_date,end_date):
        session = self.open_blpapi_session()
        if session is None:
            self.last_error = "failed to open bloomberg session"
            return False
        # open service
        svc = "//blp/refdata"
        service = self.open_service(session, svc)
        if service is None:
            self.last_error = "failed to open bloomberg service " + svc
            return False
        bbg_request = service.createRequest("HistoricalDataRequest")
        # append securities to request
        if sec_id_type == "BBGID":
            for bbgid in sec_ids:
                bbg_request.append("securities", "/bbgid/" + bbgid)
        else: # assume non BBGID is ticker
            for ticker in sec_ids:
                bbg_request.append("securities", ticker)
        # append fields to request
        for fld in fields:
            bbg_request.append("fields",fld)

        # add dates
        bbg_request.set("startDate", start_date)
        bbg_request.set("endDate", end_date)

        # add overrides
        overrides = bbg_request.getElement("overrides")
        for key in overrides_dict:
            fieldId = key
            value = overrides_dict[key]
            ovrd = overrides.appendElement()
            ovrd.setElement("fieldId", fieldId)
            ovrd.setElement("value", value)

        cid = session.sendRequest(bbg_request)

        api_msg_list = []
        try:
            # Process received events
            while True:
                # We provide timeout to give the chance to Ctrl+C handling:
                event = session.nextEvent(500)
                for msg in event:
                    if cid in msg.correlationIds():
                        api_msg = self.processHistoricalDataResoponse(msg)
                        api_msg_list.append(api_msg)

                # Response completely received, so we could exit
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            # Stop the session
            session.stop()
            return api_msg_list

    def processHistoricalDataResoponse(self, msg):
        if not msg.hasElement(SECURITY_DATA):
            self.last_error = "Unexpected message: " + msg
            return None
        api_msg = apiMsg()
        securityData = msg.getElement(SECURITY_DATA)
        if not securityData.hasElement(FIELD_DATA):
            self.last_error = "Invalid request. fieldData empty "
            return None
        sec_name = securityData.getElementAsString(SECURITY)
        api_msg.add_security(sec_name)
        fieldDataArray = securityData.getElement(FIELD_DATA).values()
        for histField in fieldDataArray:
            for elem in histField.elements():
                api_msg.add_field(sec_name, str(elem.name()), elem.getValueAsString())
        # handle errors
        for fieldException in securityData.getElement(FIELD_EXCEPTIONS).values():
            errorInfo = fieldException.getElement(ERROR_INFO)
            category = errorInfo.getElementAsString("category")
            err_value = fieldException.getElementAsString(FIELD_ID)
            api_msg.add_error(sec_name, category, err_value)

        return api_msg
