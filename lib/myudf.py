def age_validation(age):
    if age is not None:
        if (age < 13):
             return "Children"
        elif age >=13 and age <=18:
            return "Teen"
        else:
            return "Adult"
    else:
        return "Invalid Age"