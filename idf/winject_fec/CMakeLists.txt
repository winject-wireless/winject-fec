# ESP-IDF component: winject-fec encoder/decoder with ISA-L's portable erasure code and the
# optional ESP32-P4 PIE kernel. Add <winject-fec>/idf/winject_fec to EXTRA_COMPONENT_DIRS.
#
# bfc: the top-level project sets BFC_PIN and BFC_INCLUDE_DIRS (or BFC_CONFIG_DIR) before the
# component is processed, as for vstreamer and winject-l3.
set(WINJECT_FEC_ROOT ${CMAKE_CURRENT_LIST_DIR}/../..)

set(WINJECT_FEC_SOURCES
    ${WINJECT_FEC_ROOT}/src/wire.cpp
    ${WINJECT_FEC_ROOT}/src/rs_common.cpp
    ${WINJECT_FEC_ROOT}/src/rs_encode_scalar.cpp
    ${WINJECT_FEC_ROOT}/src/encoder.cpp
    ${WINJECT_FEC_ROOT}/src/decoder.cpp
)

# ISA-L and bfc are fetched after IDF's early requirement scan.
set(WINJECT_FEC_ISAL_SOURCES)
set(WINJECT_FEC_ISAL_INCLUDES)
set(WINJECT_FEC_BFC_INCLUDES)
if(NOT CMAKE_BUILD_EARLY_EXPANSION)
    include(${WINJECT_FEC_ROOT}/cmake/bfc.cmake)
    set(WINJECT_FEC_BFC_INCLUDES ${BFC_INCLUDE_DIRS})
    include(${WINJECT_FEC_ROOT}/cmake/isal_ec.cmake)
    winject_fec_fetch_isal()
    set(WINJECT_FEC_ISAL_SOURCES ${WINJECT_FEC_ISAL_ROOT}/erasure_code/ec_base.c)
    set(WINJECT_FEC_ISAL_INCLUDES ${WINJECT_FEC_ISAL_ROOT}/include
                                  ${WINJECT_FEC_ISAL_ROOT}/erasure_code)
endif()

idf_component_register(
    SRCS ${WINJECT_FEC_SOURCES}
         ${WINJECT_FEC_ISAL_SOURCES}
    INCLUDE_DIRS ${WINJECT_FEC_ROOT}/include
                 ${WINJECT_FEC_BFC_INCLUDES}
    PRIV_INCLUDE_DIRS ${WINJECT_FEC_ROOT}/src
                      ${WINJECT_FEC_ISAL_INCLUDES}
    PRIV_REQUIRES esp_hw_support heap
)

target_compile_options(${COMPONENT_LIB} PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-std=gnu++17>
    # bfc throws on fatal errors (allocation); with IDF's C++ exceptions off a throw aborts.
    $<$<COMPILE_LANGUAGE:CXX>:-fexceptions>
)
if(WINJECT_FEC_ISAL_SOURCES)
    set_source_files_properties(${WINJECT_FEC_ISAL_SOURCES}
        PROPERTIES COMPILE_DEFINITIONS NDEBUG)
endif()
