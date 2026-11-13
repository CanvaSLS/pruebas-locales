class ClientError(Exception):
    def __init__(self, error_response, operation_name):
        self.response = error_response
        self.operation_name = operation_name
        super().__init__(f"An error occurred ({error_response['Error']['Code']}) when calling the "
                         f"{operation_name} operation: {error_response['Error'].get('Message', '')}")
