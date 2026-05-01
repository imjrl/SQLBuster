"""
SQL Function Registry Module

This module defines the SQL function registry (FunctionRegistry) and related data structures,
used to manage SQL function information, supporting SQL-92 standard functions and user-defined functions.
Provides function querying, validation, and SQL generation capabilities.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union


@dataclass
class SQLFunctionParameter:
    """
    SQL Function Parameter Definition

    Represents a parameter of an SQL function, including parameter name, type, and other attributes.
    """

    name: str
    """Parameter name"""

    param_type: str
    """Parameter type, e.g. 'string', 'numeric', 'integer', 'datetime', 'any'"""

    is_optional: bool = False
    """Whether the parameter is optional, defaults to False"""

    is_variadic: bool = False
    """Whether the parameter is variadic (e.g. COALESCE's parameters), defaults to False"""

    default_value: Optional[Any] = None
    """Default value for optional parameters"""


@dataclass
class SQLFunction:
    """
    SQL Function Definition

    Represents an SQL function, including function name, return type, parameter list, and SQL template.
    """

    name: str
    """Function name, e.g. 'UPPER', 'COUNT', 'SUM'"""

    return_type: str
    """Return type, e.g. 'string', 'numeric', 'integer', 'datetime', 'any'"""

    parameters: List[SQLFunctionParameter]
    """List of parameters"""

    sql_template: str
    """SQL template using {param_name} as placeholders, e.g. 'UPPER({column})'"""

    category: str = "general"
    """
    Function category for grouping:
    - 'aggregate': Aggregate functions
    - 'string': String functions
    - 'numeric': Numeric functions
    - 'datetime': Datetime functions
    - 'conversion': Conversion functions
    - 'conditional': Conditional functions
    - 'custom': User-defined functions
    """

    is_aggregate: bool = False
    """Whether the function is an aggregate function, defaults to False"""

    supports_distinct: bool = False
    """Whether the function supports DISTINCT keyword (aggregate functions only), defaults to False"""

    description: str = ""
    """Function description"""


class FunctionRegistry:
    """
    SQL Function Registry

    Manages registration, querying, and validation of SQL functions.
    Pre-registers SQL-92 standard functions and supports user-defined functions.
    """

    def __init__(self) -> None:
        """Initialize function registry"""
        self._functions: Dict[str, SQLFunction] = {}
        self._functions_by_category: Dict[str, List[str]] = {}
        self._initialize_sql92_functions()

    def _initialize_sql92_functions(self) -> None:
        """Initialize SQL-92 standard functions"""
        # Aggregate Functions
        self._register_aggregate_functions()

        # String Functions
        self._register_string_functions()

        # Numeric Functions
        self._register_numeric_functions()

        # Datetime Functions
        self._register_datetime_functions()

        # Conversion Functions
        self._register_conversion_functions()

        # Conditional Functions
        self._register_conditional_functions()

    def _register_aggregate_functions(self) -> None:
        """Register aggregate functions"""
        # COUNT(*)
        self.register_function(
            SQLFunction(
                name="COUNT_STAR",
                return_type="integer",
                parameters=[],
                sql_template="COUNT(*)",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=False,
                description="Count all rows",
            )
        )

        # COUNT(expr)
        self.register_function(
            SQLFunction(
                name="COUNT",
                return_type="integer",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="any"),
                ],
                sql_template="COUNT({expr})",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=True,
                description="Count non-null values of expr",
            )
        )

        # COUNT(DISTINCT expr)
        self.register_function(
            SQLFunction(
                name="COUNT_DISTINCT",
                return_type="integer",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="any"),
                ],
                sql_template="COUNT(DISTINCT {expr})",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=False,
                description="Count distinct non-null values of expr",
            )
        )

        # SUM(expr)
        self.register_function(
            SQLFunction(
                name="SUM",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="numeric"),
                ],
                sql_template="SUM({expr})",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=True,
                description="Sum of values",
            )
        )

        # AVG(expr)
        self.register_function(
            SQLFunction(
                name="AVG",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="numeric"),
                ],
                sql_template="AVG({expr})",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=True,
                description="Average of values",
            )
        )

        # MAX(expr)
        self.register_function(
            SQLFunction(
                name="MAX",
                return_type="any",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="any"),
                ],
                sql_template="MAX({expr})",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=False,
                description="Maximum value",
            )
        )

        # MIN(expr)
        self.register_function(
            SQLFunction(
                name="MIN",
                return_type="any",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="any"),
                ],
                sql_template="MIN({expr})",
                category="aggregate",
                is_aggregate=True,
                supports_distinct=False,
                description="Minimum value",
            )
        )

    def _register_string_functions(self) -> None:
        """Register string functions"""
        # UPPER(string)
        self.register_function(
            SQLFunction(
                name="UPPER",
                return_type="string",
                parameters=[
                    SQLFunctionParameter(name="string", param_type="string"),
                ],
                sql_template="UPPER({string})",
                category="string",
                description="Convert string to uppercase",
            )
        )

        # LOWER(string)
        self.register_function(
            SQLFunction(
                name="LOWER",
                return_type="string",
                parameters=[
                    SQLFunctionParameter(name="string", param_type="string"),
                ],
                sql_template="LOWER({string})",
                category="string",
                description="Convert string to lowercase",
            )
        )

        # TRIM(string)
        self.register_function(
            SQLFunction(
                name="TRIM",
                return_type="string",
                parameters=[
                    SQLFunctionParameter(name="string", param_type="string"),
                ],
                sql_template="TRIM({string})",
                category="string",
                description="Remove leading and trailing spaces",
            )
        )

        # SUBSTRING(string FROM start [FOR length])
        self.register_function(
            SQLFunction(
                name="SUBSTRING",
                return_type="string",
                parameters=[
                    SQLFunctionParameter(name="string", param_type="string"),
                    SQLFunctionParameter(name="start", param_type="integer"),
                    SQLFunctionParameter(
                        name="length",
                        param_type="integer",
                        is_optional=True,
                        default_value=None,
                    ),
                ],
                sql_template="SUBSTRING({string} FROM {start}{length_clause})",
                category="string",
                description="Extract substring",
            )
        )

        # CHAR_LENGTH(string) / CHARACTER_LENGTH(string)
        self.register_function(
            SQLFunction(
                name="CHAR_LENGTH",
                return_type="integer",
                parameters=[
                    SQLFunctionParameter(name="string", param_type="string"),
                ],
                sql_template="CHAR_LENGTH({string})",
                category="string",
                description="Get string length in characters",
            )
        )

        # CONCAT(string1, string2, ...)
        self.register_function(
            SQLFunction(
                name="CONCAT",
                return_type="string",
                parameters=[
                    SQLFunctionParameter(
                        name="strings", param_type="string", is_variadic=True
                    ),
                ],
                sql_template="CONCAT({strings})",
                category="string",
                description="Concatenate strings",
            )
        )

    def _register_numeric_functions(self) -> None:
        """Register numeric functions"""
        # ABS(numeric)
        self.register_function(
            SQLFunction(
                name="ABS",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="numeric", param_type="numeric"),
                ],
                sql_template="ABS({numeric})",
                category="numeric",
                description="Absolute value",
            )
        )

        # MOD(integer, integer)
        self.register_function(
            SQLFunction(
                name="MOD",
                return_type="integer",
                parameters=[
                    SQLFunctionParameter(name="n", param_type="integer"),
                    SQLFunctionParameter(name="m", param_type="integer"),
                ],
                sql_template="MOD({n}, {m})",
                category="numeric",
                description="Remainder of n divided by m",
            )
        )

        # ROUND(numeric [, integer])
        self.register_function(
            SQLFunction(
                name="ROUND",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="numeric", param_type="numeric"),
                    SQLFunctionParameter(
                        name="decimals",
                        param_type="integer",
                        is_optional=True,
                        default_value=None,
                    ),
                ],
                sql_template="ROUND({numeric}{decimals_clause})",
                category="numeric",
                description="Round numeric to specified decimal places",
            )
        )

        # FLOOR(numeric)
        self.register_function(
            SQLFunction(
                name="FLOOR",
                return_type="integer",
                parameters=[
                    SQLFunctionParameter(name="numeric", param_type="numeric"),
                ],
                sql_template="FLOOR({numeric})",
                category="numeric",
                description="Largest integer not greater than numeric",
            )
        )

        # CEILING(numeric) / CEIL(numeric)
        self.register_function(
            SQLFunction(
                name="CEILING",
                return_type="integer",
                parameters=[
                    SQLFunctionParameter(name="numeric", param_type="numeric"),
                ],
                sql_template="CEILING({numeric})",
                category="numeric",
                description="Smallest integer not less than numeric",
            )
        )

        # POWER(numeric, exponent)
        self.register_function(
            SQLFunction(
                name="POWER",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="numeric", param_type="numeric"),
                    SQLFunctionParameter(name="exponent", param_type="numeric"),
                ],
                sql_template="POWER({numeric}, {exponent})",
                category="numeric",
                description="Raise numeric to the power of exponent",
            )
        )

        # SQRT(numeric)
        self.register_function(
            SQLFunction(
                name="SQRT",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="numeric", param_type="numeric"),
                ],
                sql_template="SQRT({numeric})",
                category="numeric",
                description="Square root of numeric",
            )
        )

    def _register_datetime_functions(self) -> None:
        """Register datetime functions"""
        # CURRENT_DATE
        self.register_function(
            SQLFunction(
                name="CURRENT_DATE",
                return_type="date",
                parameters=[],
                sql_template="CURRENT_DATE",
                category="datetime",
                description="Current date",
            )
        )

        # CURRENT_TIME
        self.register_function(
            SQLFunction(
                name="CURRENT_TIME",
                return_type="time",
                parameters=[],
                sql_template="CURRENT_TIME",
                category="datetime",
                description="Current time",
            )
        )

        # CURRENT_TIMESTAMP
        self.register_function(
            SQLFunction(
                name="CURRENT_TIMESTAMP",
                return_type="timestamp",
                parameters=[],
                sql_template="CURRENT_TIMESTAMP",
                category="datetime",
                description="Current timestamp",
            )
        )

        # EXTRACT(field FROM datetime)
        self.register_function(
            SQLFunction(
                name="EXTRACT",
                return_type="numeric",
                parameters=[
                    SQLFunctionParameter(name="field", param_type="string"),
                    SQLFunctionParameter(name="datetime", param_type="datetime"),
                ],
                sql_template="EXTRACT({field} FROM {datetime})",
                category="datetime",
                description="Extract field from datetime",
            )
        )

    def _register_conversion_functions(self) -> None:
        """Register conversion functions"""
        # CAST(expr AS type)
        self.register_function(
            SQLFunction(
                name="CAST",
                return_type="any",
                parameters=[
                    SQLFunctionParameter(name="expr", param_type="any"),
                    SQLFunctionParameter(name="type", param_type="string"),
                ],
                sql_template="CAST({expr} AS {type})",
                category="conversion",
                description="Cast expr to specified type",
            )
        )

    def _register_conditional_functions(self) -> None:
        """Register conditional functions"""
        # COALESCE(value1, value2, ...)
        self.register_function(
            SQLFunction(
                name="COALESCE",
                return_type="any",
                parameters=[
                    SQLFunctionParameter(
                        name="values", param_type="any", is_variadic=True
                    ),
                ],
                sql_template="COALESCE({values})",
                category="conditional",
                description="Return first non-null value",
            )
        )

        # NULLIF(value1, value2)
        self.register_function(
            SQLFunction(
                name="NULLIF",
                return_type="any",
                parameters=[
                    SQLFunctionParameter(name="value1", param_type="any"),
                    SQLFunctionParameter(name="value2", param_type="any"),
                ],
                sql_template="NULLIF({value1}, {value2})",
                category="conditional",
                description="Return NULL if value1 equals value2, otherwise value1",
            )
        )

    def register_function(self, func: SQLFunction) -> None:
        """
        Register SQL function

        Args:
            func: SQL function definition

        Raises:
            ValueError: If function name already exists
        """
        if func.name in self._functions:
            raise ValueError(f"Function '{func.name}' already exists")

        self._functions[func.name] = func

        if func.category not in self._functions_by_category:
            self._functions_by_category[func.category] = []
        if func.name not in self._functions_by_category[func.category]:
            self._functions_by_category[func.category].append(func.name)

    def register_custom_function(
        self,
        name: str,
        return_type: str,
        parameters: List[Dict[str, Any]],
        sql_template: str,
        description: str = "",
    ) -> None:
        """
        Register user-defined function

        Args:
            name: Function name
            return_type: Return type
            parameters: List of parameter dictionaries, each containing:
                - 'name': Parameter name
                - 'type': Parameter type
                - 'optional' (optional): Whether parameter is optional
                - 'variadic' (optional): Whether parameter is variadic
                - 'default' (optional): Default value
            sql_template: SQL template using {param_name} as placeholders
            description: Function description

        Raises:
            ValueError: If function name already exists or parameter format is invalid
        """
        # Convert parameters to SQLFunctionParameter objects
        func_params = []
        for param_dict in parameters:
            if "name" not in param_dict or "type" not in param_dict:
                raise ValueError("Parameters must contain 'name' and 'type' fields")

            func_params.append(
                SQLFunctionParameter(
                    name=param_dict["name"],
                    param_type=param_dict["type"],
                    is_optional=param_dict.get("optional", False),
                    is_variadic=param_dict.get("variadic", False),
                    default_value=param_dict.get("default", None),
                )
            )

        func = SQLFunction(
            name=name,
            return_type=return_type,
            parameters=func_params,
            sql_template=sql_template,
            category="custom",
            description=description,
        )

        self.register_function(func)

    def get_function(self, name: str) -> Optional[SQLFunction]:
        """
        Get function definition.

        Args:
            name: Function name

        Returns:
            Function definition, or None if not found
        """
        return self._functions.get(name)

    def get_functions_by_category(self, category: str) -> List[SQLFunction]:
        """
        Get list of functions by category.

        Args:
            category: Function category

        Returns:
            List of functions in the specified category
        """
        func_names = self._functions_by_category.get(category, [])
        return [self._functions[name] for name in func_names if name in self._functions]

    def get_all_functions(self) -> List[SQLFunction]:
        """
        Get all registered functions.

        Returns:
            List of all registered functions
        """
        return list(self._functions.values())

    def get_all_categories(self) -> List[str]:
        """
        Get all function categories.

        Returns:
            List of all function categories
        """
        return list(self._functions_by_category.keys())

    def generate_sql(
        self, func_name: str, args: Dict[str, Any], use_distinct: bool = False
    ) -> Optional[str]:
        """
        Generate SQL for function call.

        Args:
            func_name: Function name
            args: Dictionary of parameter names to values
            use_distinct: Whether to use DISTINCT (valid for aggregate functions only)

        Returns:
            Generated SQL string, or None if generation fails

        Examples:
            generate_sql('UPPER', {'string': 'name'}) -> "UPPER(name)"
            generate_sql('COUNT', {'expr': 'age', 'distinct': True}) -> "COUNT(DISTINCT age)"
        """
        func = self.get_function(func_name)
        if func is None:
            return None

        # Build template argument dictionary
        template_args = {}

        for param in func.parameters:
            if param.is_variadic:
                # Variadic parameters
                if param.name in args:
                    values = args[param.name]
                    if isinstance(values, (list, tuple)):
                        template_args[param.name] = ", ".join(str(v) for v in values)
                    else:
                        template_args[param.name] = str(values)
                else:
                    template_args[param.name] = ""
            elif param.is_optional:
                # Optional parameter not provided
                if func.sql_template.find("{" + param.name + "_clause}") >= 0:
                    # Handle optional parameters using _clause suffix
                    template_args[param.name + "_clause"] = ""
                template_args[param.name] = ""
            else:
                # Required parameter not provided
                return None

        # Handle clauses for optional parameters
        for param in func.parameters:
            if param.is_optional and param.name not in args:
                # Optional parameter not provided, clear clause
                clause_key = param.name + "_clause"
                if func.sql_template.find("{" + clause_key + "}") >= 0:
                    template_args[clause_key] = ""
                template_args[param.name] = ""

        # Special handling for optional parameter clauses
        if func.name == "SUBSTRING":
            if "length" in args and args["length"] is not None:
                template_args["length_clause"] = f" FOR {args['length']}"
            else:
                template_args["length_clause"] = ""
        elif func.name == "ROUND":
            if "decimals" in args and args["decimals"] is not None:
                template_args["decimals_clause"] = f", {args['decimals']}"
            else:
                template_args["decimals_clause"] = ""

        # Handle DISTINCT
        sql = func.sql_template.format(**template_args)

        if use_distinct and func.supports_distinct:
            # Insert DISTINCT before first function parameter
            # Simple handling: directly replace function name with function name(DISTINCT ...)
            import re

            pattern = re.compile(rf"^{func.name}\((.*)\)$", re.IGNORECASE)
            match = pattern.match(sql)
            if match:
                inner = match.group(1)
                sql = f"{func.name}(DISTINCT {inner})"

        return sql

    def validate_function_call(
        self, func_name: str, args: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate function call validity.

        Args:
            func_name: Function name
            args: Dictionary of parameter names to values

        Returns:
            Tuple of (is_valid, error_message)
        """
        func = self.get_function(func_name)
        if func is None:
            return False, f"Function '{func_name}' does not exist"

            # Check required parameters
        for param in func.parameters:
            if not param.is_optional and not param.is_variadic:
                if param.name not in args:
                    return False, f"Missing required parameter: '{param.name}'"

        return True, None

    def list_functions(self) -> str:
        """
        List all registered functions (for debugging).

        Returns:
            Formatted string of all registered functions
        """
        lines = ["Registered SQL Functions:", "=" * 50]

        for category in sorted(self._functions_by_category.keys()):
            lines.append(f"\n[{category}]")
            for func_name in self._functions_by_category[category]:
                func = self._functions[func_name]
                params_str = ", ".join(
                    f"{p.name}: {p.param_type}{' (optional)' if p.is_optional else ''}"
                    for p in func.parameters
                )
                lines.append(f"  {func.name}({params_str}) -> {func.return_type}")
                if func.description:
                    lines.append(f"    {func.description}")

        return "\n".join(lines)
