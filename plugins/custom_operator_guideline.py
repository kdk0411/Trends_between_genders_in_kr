from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class Custom_Operator_Guideline(BaseOperator):
    
    @apply_defaults
    def __init__(self, param_1, param_2, *args, **kawrgs):
        super(Custom_Operator_Guideline, self).__init__(*args, **kawrgs)
        self.param_1 = param_1
        self.param_2 = param_2

    def execute(self, context):
        self.log.info(f"Executing Custom Operator Guideline with param_1 : {self.param_1} and param_2 : {self.param_2}")
        
        ####################
        #Make Your Function#
        ####################

    