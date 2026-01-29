# Role-Based Access Control Configuration
# Define which users can access which roles in the application

# Users authorized to use MAKER role
AUTHORIZED_MAKERS = [
    "gilbert.lavides@paymaya.com",
    # Add more maker emails here
]

# Users authorized to use CHECKER role
AUTHORIZED_CHECKERS = [
    # Add checker emails here
]

# Admin users (can access both roles)
AUTHORIZED_ADMINS = [
    # Add admin emails here
]

def get_user_roles(user_email: str) -> list:
    """
    Get list of roles a user is authorized for.
    Returns: List of role names ['MAKER', 'CHECKER'] or empty list
    """
    roles = []
    
    # Check if user is admin (has all roles)
    if user_email in AUTHORIZED_ADMINS:
        return ["MAKER", "CHECKER"]
    
    # Check specific roles
    if user_email in AUTHORIZED_MAKERS:
        roles.append("MAKER")
    
    if user_email in AUTHORIZED_CHECKERS:
        roles.append("CHECKER")
    
    return roles

def is_authorized_for_role(user_email: str, role: str) -> bool:
    """
    Check if a user is authorized for a specific role.
    
    Args:
        user_email: User's email address
        role: Role name ('MAKER' or 'CHECKER')
    
    Returns:
        True if authorized, False otherwise
    """
    authorized_roles = get_user_roles(user_email)
    return role in authorized_roles

def get_default_role(user_email: str) -> str:
    """
    Get the default role for a user.
    Returns the first available role or None if no roles.
    """
    roles = get_user_roles(user_email)
    return roles[0] if roles else None
